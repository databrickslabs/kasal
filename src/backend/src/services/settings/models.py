"""
Service for model configuration operations.

This module provides business logic for model configuration operations,
including retrieving and managing model configurations.

PERFORMANCE: Uses TTL caching for group-scoped model queries with automatic
invalidation on mutations. See src/core/cache.py for cache implementation.
"""

import asyncio
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache import endpoint_health_cache, model_config_cache
from src.core.exceptions import ForbiddenError, KasalError
from src.core.logger import LoggerManager
from src.models.model_config import ModelConfig
from src.repositories.model_config_repository import ModelConfigRepository
from src.services.llm.endpoints import SELF_HOSTED_DEFAULTS, model_api_base
from src.services.settings.api_keys import ApiKeysService
from src.utils.model_config import get_model_config
from src.utils.user_context import GroupContext

logger = LoggerManager.get_instance().crew


def _databricks_configured() -> bool:
    """True when a Databricks workspace is reachable via environment.

    Databricks Apps and PAT/SP setups always export at least one of these; a
    purely-local deployment exports none. Used to decide whether a Databricks
    model can actually authenticate — if not, system/auxiliary LLM ops fall back
    to the enabled local model instead of failing over to OpenAI (→ 401).
    """
    return any(
        os.getenv(v)
        for v in (
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN",
            "DATABRICKS_API_KEY",
            "DATABRICKS_CLIENT_ID",
        )
    )


# Self-hosted providers are preferred for the fallback because they cost
# nothing to call — but only when the box actually answers. Their endpoint is
# the model's own (Configuration → Models; see services/llm/endpoints.py).

# Ranking for fallback candidates: self-hosted first (free), then hosted models
# that have a usable API key.
_FALLBACK_RANK = {"vllm": 0, "ollama": 1, "custom": 2}
_HOSTED_RANK = 3

# How long to wait for a TCP connect when probing a self-hosted endpoint. Long
# enough for a LAN box, short enough that a dead host costs a blink.
_PROBE_TIMEOUT_SECONDS = 0.75

#: Columns a group's override row copies from the catalog row it overrides.
_OVERRIDE_COPY_FIELDS = (
    "key",
    "name",
    "provider",
    "temperature",
    "context_window",
    "max_output_tokens",
    "extended_thinking",
    "thinking_budget_tokens",
    "reasoning_effort",
    "params",
    "unsupported_params",
)


async def _endpoint_reachable(url: str) -> bool:
    """True when a TCP connection to the endpoint's host:port succeeds.

    Cheap liveness signal (no HTTP round trip), cached for 60s. "Enabled" in the
    catalogue does not mean "running": a self-hosted box that is powered off
    still looks enabled, and picking it stalls every generation on a multi-minute
    timeout instead of using a hosted model that answers immediately.
    """
    try:
        parsed = urlparse(url)
        host, port = parsed.hostname, parsed.port
        if not host:
            return False
        if port is None:
            port = 443 if parsed.scheme == "https" else 80
    except Exception:  # noqa: BLE001 — a malformed URL is simply unreachable
        return False

    cache_key = f"{host}:{port}"
    cached = await endpoint_health_cache.get("__default__", cache_key)
    if cached is not None:
        return bool(cached)

    reachable = False
    writer = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=_PROBE_TIMEOUT_SECONDS
        )
        reachable = True
    except (asyncio.TimeoutError, OSError):
        reachable = False
    except Exception as e:  # noqa: BLE001 — probing must never break resolution
        logger.debug(f"Endpoint probe for {cache_key} failed unexpectedly: {e}")
        reachable = False
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass

    await endpoint_health_cache.set("__default__", cache_key, reachable)
    if not reachable:
        logger.warning(f"Self-hosted endpoint {cache_key} is not reachable")
    return reachable


class ModelConfigService:
    """Service for model configuration operations."""

    def __init__(self, session: AsyncSession, group_id: Optional[str] = None):
        """
        Initialize the service with session.

        Args:
            session: Database session
            group_id: Group ID for multi-tenant isolation (optional for read operations,
                     REQUIRED for API key operations)

        Note:
            group_id is optional for reading model configurations (which are not tenant-specific),
            but REQUIRED for any operations involving API keys (multi-tenant isolation).
        """
        self.repository = ModelConfigRepository(session)
        # SECURITY: Store group_id for multi-tenant API key isolation
        # Will be validated when needed (e.g., in API key operations)
        self.group_id = group_id

    async def find_all(self) -> List[ModelConfig]:
        """
        Get all model configurations from the repository.

        Returns:
            List of all model configurations
        """
        return await self.repository.find_all()

    async def find_enabled_models(self) -> List[ModelConfig]:
        """
        Get all enabled model configurations from the repository.

        Returns:
            List of enabled model configurations
        """
        return await self.repository.find_enabled_models()

    async def find_by_key(self, key: str) -> Optional[ModelConfig]:
        """
        Get a model configuration by its key: this service's group's own row
        when it has one, else the global row. Nothing else is returned.

        Args:
            key: The model key to find

        Returns:
            Model configuration if found, None otherwise
        """
        rows = await self._resolve_rows(key, key)
        if rows is None:
            return None
        row, definition = rows
        return row if definition is row else self._overlay(definition, row)

    async def create_model_config(
        self, model_data: Any, group_id: Optional[str] = None
    ) -> ModelConfig:
        """
        Create a new model configuration.

        Args:
            model_data: Data for the new model configuration
            group_id: Optional group ID for cache invalidation

        Returns:
            Created model configuration

        Raises:
            ValueError: If model with the same key already exists
        """
        # Check if model already exists
        existing_model = await self.repository.find_by_key(model_data.key)
        if existing_model:
            raise ValueError(f"Model with key {model_data.key} already exists")

        # Convert Pydantic model to dict if needed
        if hasattr(model_data, "model_dump"):
            model_dict = model_data.model_dump()
        elif hasattr(model_data, "dict"):
            model_dict = model_data.dict()
        else:
            model_dict = dict(model_data)

        # Create new model
        result = await self.repository.create(model_dict)

        # A catalog row is every group's: drop every group's entries.
        await model_config_cache.clear()
        logger.info("[CACHE INVALIDATE] Model config cache cleared after create")

        return result

    async def update_model_config(
        self, key: str, model_data: Any, group_id: Optional[str] = None
    ) -> Optional[ModelConfig]:
        """
        Update an existing catalog (global) model configuration.

        Args:
            key: Key of the model to update
            model_data: Updated model data
            group_id: Unused; kept for callers. Every group's cache is dropped.

        Returns:
            Updated model configuration, or None if not found
        """
        # The catalog row, never a group's override of it.
        existing_model = await self.repository.find_global_by_key(key)
        if not existing_model:
            return None

        # Convert Pydantic model to dict if needed
        if hasattr(model_data, "model_dump"):
            model_dict = model_data.model_dump(exclude_unset=True)
        elif hasattr(model_data, "dict"):
            model_dict = model_data.dict(exclude_unset=True)
        else:
            model_dict = dict(model_data)

        # Update model
        result = await self.repository.update(existing_model.id, model_dict)

        # Every group resolves this row: drop every group's entries.
        await model_config_cache.clear()
        logger.info("[CACHE INVALIDATE] Model config cache cleared after update")

        return result

    async def toggle_model_enabled(
        self, key: str, enabled: bool, group_id: Optional[str] = None
    ) -> Optional[ModelConfig]:
        """
        Toggle the enabled status of a model configuration.

        Args:
            key: Key of the model to toggle
            enabled: New enabled status
            group_id: Optional group ID for cache invalidation

        Returns:
            Updated model configuration, or None if not found
        """
        try:
            # Use the direct DML method to avoid locking
            updated = await self.repository.toggle_enabled(key, enabled)

            if not updated:
                return None

            # Get the updated (global) model
            result = await self.repository.find_by_key(key)

            await model_config_cache.clear()
            logger.info("[CACHE INVALIDATE] Model config cache cleared after toggle")

            return result
        except Exception as e:
            # Log the error at service level but don't expose internal details
            logger.error(f"Error in toggle_model_enabled for key={key}: {str(e)}")
            # Re-raise for controller layer to handle
            raise

    async def delete_model_config(
        self, key: str, group_id: Optional[str] = None
    ) -> bool:
        """
        Delete a model configuration.

        Args:
            key: Key of the model to delete
            group_id: Optional group ID for cache invalidation

        Returns:
            True if deleted, False if not found
        """
        logger.info(f"Service: Attempting to delete model with key: {key}")

        # Deletes the catalog row and every group's override of it.
        result = await self.repository.delete_by_key(key)

        if result:
            await model_config_cache.clear()
            logger.info("[CACHE INVALIDATE] Model config cache cleared after delete")

        return result

    async def enable_all_models(self) -> List[ModelConfig]:
        """
        Enable all model configurations.

        Returns:
            List of all model configurations after enabling
        """
        try:
            # Enable all models with a single operation
            success = await self.repository.enable_all_models()
            if not success:
                logger.warning("Failed to enable all models")

            # Invalidate entire cache (affects all groups)
            await model_config_cache.clear()
            logger.info(
                "[CACHE INVALIDATE] Model config cache cleared after enable_all"
            )

            # Return all models
            return await self.find_all()
        except Exception as e:
            logger.error(f"Error enabling all models: {str(e)}")
            raise

    async def disable_all_models(self) -> List[ModelConfig]:
        """
        Disable all model configurations.

        Returns:
            List of all model configurations after disabling
        """
        try:
            # Disable all models with a single operation
            success = await self.repository.disable_all_models()
            if not success:
                logger.warning("Failed to disable all models")

            # Invalidate entire cache (affects all groups)
            await model_config_cache.clear()
            logger.info(
                "[CACHE INVALIDATE] Model config cache cleared after disable_all"
            )

            # Return all models
            return await self.find_all()
        except Exception as e:
            logger.error(f"Error disabling all models: {str(e)}")
            raise

    async def get_model_config(self, model: str) -> Dict[str, Any]:
        """
        Get configuration for a specific model.

        Args:
            model: Name of the model to get configuration for

        Returns:
            Dictionary containing model configuration

        Raises:
            HTTPException: If model configuration is not found
        """
        try:
            normalized_key = (
                model.rsplit("/", 1)[-1] if isinstance(model, str) else model
            )

            # Read-through TTL cache (PERF-005): this runs on EVERY LLM
            # completion, which previously meant a DB round trip per call.
            # Keyed by THIS group: the row it resolves to is the group's own.
            # API keys are never cached here — only the model row fields.
            cache_group = self.group_id or "__default__"
            cached = await model_config_cache.get(
                cache_group, f"model:{normalized_key}"
            )
            if cached is not None:
                config = dict(cached)
            else:
                rows = await self._resolve_rows(model, normalized_key)
                if rows:
                    row, definition = rows
                    config = self._as_config(definition)
                    config["enabled"] = row.enabled
                else:
                    # Fall back to utility function with normalized key (best-effort; may be None without a sync DB session)
                    config = get_model_config(normalized_key) or {}
                    if not config:
                        raise ValueError(
                            f"Model configuration not found for model: {model}"
                        )
                await model_config_cache.set(
                    cache_group, f"model:{normalized_key}", dict(config)
                )

            # Get API key for the provider using class method
            provider = config["provider"].lower()

            # Check if we're using Databricks provider - unified auth handles it
            if provider == "databricks":
                # LOCAL FALLBACK: on a non-Databricks deployment, Databricks-provider
                # models (the dispatcher / crew-agent-task generation / planning
                # defaults, e.g. databricks-gpt-5-3-codex) can't authenticate, and
                # litellm falls back to OpenAI with whatever key is set → 401
                # "Incorrect API key". Resolve to the enabled LOCAL model instead so
                # these system/auxiliary ops run on the local stack (vLLM / Ollama).
                if not _databricks_configured():
                    local = await self._local_fallback_config()
                    if local and local.get("key") != config.get("key"):
                        logger.warning(
                            "Databricks model '%s' requested but no Databricks workspace "
                            "is configured — falling back to local model '%s'",
                            config.get("name"),
                            local.get("name"),
                        )
                        return local
                logger.info(
                    "Databricks provider - unified auth will handle authentication"
                )
                # Don't add API key for Databricks - unified auth handles it
                return config

            # vLLM is self-hosted; api_key is baked into llm_manager via env var
            if provider == "vllm":
                return config

            # Custom is self-hosted; api_key is baked into llm_manager via env var
            if provider == "custom":
                return config

            # For non-Databricks providers, get API key
            # SECURITY: group_id is REQUIRED for API key operations (multi-tenant isolation)
            if not self.group_id:
                raise ValueError(
                    f"SECURITY: group_id is REQUIRED for fetching API keys for provider '{provider}'. "
                    "All API key operations must be scoped to a group for multi-tenant isolation."
                )
            api_key = await ApiKeysService.get_provider_api_key(
                provider, group_id=self.group_id
            )
            if not api_key:
                # Try to use unified auth for external providers if available
                try:
                    from src.utils.databricks_auth import get_auth_context

                    auth = await get_auth_context()
                    if auth and auth.auth_method in ["obo", "service_principal"]:
                        logger.warning(
                            f"No API key found for provider {provider} - this may cause issues if the model requires external API access"
                        )
                        # Allow the request to proceed - the actual LLM call might fail, but that's better than failing here
                        return config
                except ImportError:
                    pass
                raise ValueError(f"No API key found for provider: {provider}")

            # Add API key to config
            config["api_key"] = api_key
            return config

        except Exception as e:
            logger.error(f"Error getting model configuration: {str(e)}")
            raise KasalError(detail=f"Failed to get model configuration: {str(e)}")

    async def _resolve_rows(
        self, model: str, normalized_key: str
    ) -> Optional[Tuple[ModelConfig, ModelConfig]]:
        """``(row, definition)`` this group resolves a model to, or None.

        ``row`` is the group's own override when it has one, else the global
        row. An override changes ``enabled`` only (the toggle is the one thing a
        group can change), so ``definition`` — how to call the model: endpoint,
        limits, sampling and thinking settings — is the global row whenever one
        exists. An override made before a system admin's edit, or one that
        never carried those columns, therefore cannot drop them.
        """
        # Try the full model name first (supports HF-style names like
        # "mlx-community/model-name"), then fall back to the normalized
        # key (strips provider prefix e.g. "databricks/model" → "model").
        row = await self.repository.find_by_key(model, self.group_id)
        if not row and normalized_key != model:
            row = await self.repository.find_by_key(normalized_key, self.group_id)
        if not row:
            return None
        definition = row
        if getattr(row, "group_id", None) is not None:
            definition = await self.repository.find_global_by_key(row.key) or row
        return row, definition

    @staticmethod
    def _overlay(definition: ModelConfig, override: ModelConfig) -> ModelConfig:
        """The catalog row as one group sees it: its definition, the group's
        ``enabled`` (and the override's id and group, which identify it).

        A new, transient object that is never added to a session, so reading
        it can never write to either row.
        """
        columns = ModelConfig.__table__.columns.keys()
        view = ModelConfig(**{c: getattr(definition, c, None) for c in columns})
        view.id = override.id
        view.group_id = override.group_id
        view.enabled = override.enabled
        return view

    @staticmethod
    def _as_config(m: Any) -> Dict[str, Any]:
        """Model row -> the config dict shape get_model_config returns.

        ``getattr`` throughout: a row here is not always a full ORM object (the
        seed-backed and cached shapes are plainer), and a missing column must
        read as "declared nothing", never raise.
        """
        return {
            "key": m.key,
            "name": m.name,
            "provider": m.provider,
            "temperature": m.temperature,
            "context_window": m.context_window,
            "max_output_tokens": m.max_output_tokens,
            "extended_thinking": getattr(m, "extended_thinking", False),
            "thinking_budget_tokens": getattr(m, "thinking_budget_tokens", None),
            "reasoning_effort": getattr(m, "reasoning_effort", None),
            "enabled": m.enabled,
            # The declared sampling surface: configure_kasal_llm reads both off
            # this dict, and a dict that omits them silently disarms it. This
            # builder also serves the SUBSTITUTION path, which is how a
            # Databricks model on a workspace-less deployment becomes the local
            # vLLM one — exactly the model whose row carries a frequency_penalty.
            "params": getattr(m, "params", None),
            "unsupported_params": getattr(m, "unsupported_params", None),
        }

    async def _fallback_candidate_usable(
        self, provider: str, params: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Can this provider actually serve a request right now?

        Self-hosted: the box must answer a TCP connect. Hosted: an API key must
        exist for this group. Anything that fails here is skipped so the
        substitution lands on a model that works instead of one that merely
        exists in the catalogue.
        """
        if provider in SELF_HOSTED_DEFAULTS:
            url = model_api_base(provider, params)
            return await _endpoint_reachable(url) if url else False
        if not self.group_id:
            return False
        try:
            return bool(
                await ApiKeysService.get_provider_api_key(
                    provider, group_id=self.group_id
                )
            )
        except Exception as e:  # noqa: BLE001 — fallback must never raise
            logger.debug(f"API-key lookup for fallback provider {provider} failed: {e}")
            return False

    async def _local_fallback_config(self) -> Optional[Dict[str, Any]]:
        """An enabled, *usable* non-Databricks model to substitute for a
        Databricks model on a deployment with no Databricks workspace.

        Preference is self-hosted (vllm, then ollama — they cost nothing) and
        then any hosted provider with an API key for this group. Candidates that
        cannot serve are skipped: a self-hosted box that does not answer a TCP
        connect, or a hosted provider with no key.

        This used to consider vllm/ollama ONLY, so a powered-off local box was a
        hard dead end — every generation picked it, waited out a ~226s timeout
        and failed, even with an enabled OpenAI model and a key sitting right
        there. Returns None when nothing is usable (the caller then keeps the
        original Databricks config).

        The fallback model a system admin pins (Configuration → Models → Advanced;
        it replaced KASAL_FALLBACK_MODEL) wins over the ranking when enabled.
        """
        try:
            models = await self.find_enabled_models()
        except Exception as e:  # noqa: BLE001 — fallback must never raise
            logger.warning(f"Local-fallback lookup failed: {e}")
            return None

        candidates = [m for m in models if (m.provider or "").lower() != "databricks"]

        from src.services.settings import engine_settings

        pinned_key = engine_settings.setting("fallback_model")
        if pinned_key:
            for m in candidates:
                if m.key == pinned_key:
                    logger.info(
                        "Using fallback model pin '%s' for Databricks substitution",
                        pinned_key,
                    )
                    return self._as_config(m)
            logger.warning(
                "Fallback model '%s' is not an enabled model — ignoring the pin",
                pinned_key,
            )

        ranked = sorted(
            candidates,
            key=lambda m: _FALLBACK_RANK.get((m.provider or "").lower(), _HOSTED_RANK),
        )
        for m in ranked:
            if await self._fallback_candidate_usable(
                (m.provider or "").lower(), getattr(m, "params", None)
            ):
                return self._as_config(m)

        logger.warning(
            "No usable fallback model: every enabled non-Databricks model is "
            "either an unreachable self-hosted endpoint or missing an API key."
        )
        return None

    # Group-aware methods for multi-tenant support

    async def find_all_for_group(
        self, group_context: GroupContext
    ) -> List[ModelConfig]:
        """
        Get all model configurations for a specific group.

        PERFORMANCE: Uses TTL cache (5 min) to reduce database queries.
        Cache is automatically invalidated when model configs are mutated.

        Shows:
        1. Default models (group_id = null) - visible to everyone
        2. The CURRENT workspace's models (``primary_group_id``: the selected
           workspace, or where a no-selection request creates its data)
        3. If a model has both default and group versions, the group version takes precedence

        The query is scoped to exactly the group the cache is keyed by. It used
        to merge every id in ``group_ids`` under the primary id's key, so a
        no-selection (union) request cached workspace B's models and disables
        as workspace A's list, and Auto picked from it (audit V5-2).

        Args:
            group_context: Group context with group IDs

        Returns:
            List of model configurations for the group
        """
        # The cache key IS the query scope: one workspace, or the defaults.
        scope_group_id = group_context.primary_group_id if group_context else None
        cache_group_id = scope_group_id or "__default__"

        # =========================================================================
        # TTL CACHE: Check cache first
        # =========================================================================
        cached_models: Optional[List[ModelConfig]] = await model_config_cache.get(
            cache_group_id, "models"
        )
        if cached_models is not None:
            logger.info(
                f"[CACHE HIT] Returning {len(cached_models)} cached models for group {cache_group_id}"
            )
            return cached_models

        # Cache miss - fetch from database
        logger.info(
            f"[CACHE MISS] Fetching models from database for group {cache_group_id}"
        )
        all_models = await self.repository.find_all()

        # If no group context, show only default models
        if not scope_group_id:
            default_models = [model for model in all_models if model.group_id is None]
            # Cache and return
            await model_config_cache.set(cache_group_id, "models", default_models)
            return default_models

        # Build a dictionary to handle overrides: model_key -> model
        models_by_key = {}

        # First, add all default models (group_id = null)
        for model in all_models:
            if model.group_id is None:
                models_by_key[model.key] = model

        # Then the workspace's own overrides: its `enabled` on the global row
        for model in all_models:
            if model.group_id == scope_group_id:
                base = models_by_key.get(model.key)
                models_by_key[model.key] = (
                    self._overlay(base, model) if base is not None else model
                )

        # Convert back to list
        result = list(models_by_key.values())

        # =========================================================================
        # CACHE: Store result for future requests
        # =========================================================================
        await model_config_cache.set(cache_group_id, "models", result)
        logger.info(
            f"[CACHE SET] Cached {len(result)} models for group {cache_group_id}"
        )

        return result

    async def find_enabled_models_for_group(
        self, group_context: GroupContext
    ) -> List[ModelConfig]:
        """
        Get all enabled model configurations for a specific group.

        Applies group overrides first, then filters by enabled. This ensures that
        a workspace-level disable will properly hide a model even if the global
        default is enabled.

        Args:
            group_context: Group context with group IDs

        Returns:
            List of enabled model configurations effective for the group
        """
        # First compute the effective models for the group (with overrides applied)
        effective_models = await self.find_all_for_group(group_context)

        # Then filter to only enabled ones
        return [m for m in effective_models if getattr(m, "enabled", True)]

    async def find_all_global(self) -> List[ModelConfig]:
        """Return all global (system-wide) model configurations (group_id is None)."""
        return await self.repository.find_all_global()

    async def toggle_global_enabled(
        self, key: str, enabled: bool
    ) -> Optional[ModelConfig]:
        """Toggle enabled on the global model by key (does not create group override)."""
        updated = await self.repository.toggle_global_enabled(key, enabled)
        if not updated:
            return None

        # Invalidate entire cache since global models affect all groups
        await model_config_cache.clear()
        logger.info("[CACHE INVALIDATE] Model config cache cleared after toggle_global")

        return await self.repository.find_global_by_key(key)

    async def toggle_model_enabled_with_group(
        self, key: str, enabled: bool, group_context: GroupContext
    ) -> Optional[ModelConfig]:
        """
        Toggle a model for the current group (``primary_group_id``) only.

        The first toggle stores the group's override: a complete copy of the
        catalog row with the requested ``enabled``. Later toggles flip that
        row. Only this group resolves it (see ``ModelConfigRepository.
        find_by_key``), and at run time it contributes only ``enabled``.

        Args:
            key: Key of the model to toggle
            enabled: New enabled status
            group_context: Group context with group IDs

        Returns:
            Updated model configuration, or None if not found

        Raises:
            HTTPException: If not authorized or toggle fails
        """
        try:
            # The row this group toggles: its own override of ``key``, else the
            # global row. The group is the CURRENT one (``primary_group_id``),
            # never any other id in ``group_ids``.
            primary_group_id = group_context.primary_group_id if group_context else None
            all_models = await self.repository.find_all()
            target_model = None
            default_model = None
            for model in all_models:
                if model.key != key:
                    continue
                if model.group_id is None:
                    default_model = default_model or model
                elif primary_group_id and model.group_id == primary_group_id:
                    target_model = target_model or model
            target_model = target_model or default_model

            if not target_model:
                logger.warning(f"Model with key {key} not found")
                return None

            # Must have a valid group context to toggle models
            if primary_group_id is None or group_context is None:
                logger.warning(f"No group context provided for toggling model {key}")
                raise ForbiddenError(detail="Group context required to toggle models")

            async def _invalidate_cache() -> None:
                await model_config_cache.invalidate(primary_group_id, "models")
                await model_config_cache.invalidate(primary_group_id, f"model:{key}")
                await model_config_cache.invalidate("__default__", "models")
                logger.info(
                    f"[CACHE INVALIDATE] Model config cache invalidated for group {primary_group_id}"
                )

            if target_model.group_id is None:
                existing_group_model = await self.repository.find_by_key_and_group(
                    key, primary_group_id
                )
                if not existing_group_model:
                    # The group's override: a COMPLETE copy of the catalog row
                    # (a partial one dropped the endpoint, refusals and thinking
                    # settings), resolved by this group only.
                    model_data = {
                        field: getattr(target_model, field, None)
                        for field in _OVERRIDE_COPY_FIELDS
                    }
                    model_data.update(
                        enabled=enabled,
                        group_id=primary_group_id,
                        created_by_email=group_context.group_email,
                    )
                    result = await self.repository.create(model_data)
                    await _invalidate_cache()
                    return result

            # Toggle the group's own override in its group scope
            await self.repository.toggle_enabled_in_group(
                key, primary_group_id, enabled
            )
            await _invalidate_cache()
            return await self.repository.find_by_key_and_group(key, primary_group_id)

        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle model: {str(e)}")
            raise KasalError(detail=f"Failed to toggle model: {str(e)}")
