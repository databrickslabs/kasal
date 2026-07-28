"""
Service for model configuration operations.

This module provides business logic for model configuration operations,
including retrieving and managing model configurations.

PERFORMANCE: Uses TTL caching for group-scoped model queries with automatic
invalidation on mutations. See src/core/cache.py for cache implementation.
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache import endpoint_health_cache, model_config_cache
from src.core.exceptions import ForbiddenError, KasalError, NotFoundError
from src.core.logger import LoggerManager
from src.models.model_config import ModelConfig
from src.repositories.model_config_repository import ModelConfigRepository
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


# Self-hosted providers and the env var holding their base URL (defaults mirror
# llm_manager's vLLM / Ollama branches). Preferred for the fallback because they
# cost nothing to call — but only when the box actually answers.
SELF_HOSTED_ENDPOINTS = {
    "vllm": ("VLLM_BASE_URL", "http://localhost:8081/v1"),
    "ollama": ("OLLAMA_API_BASE", "http://localhost:11434"),
}

# Ranking for fallback candidates: self-hosted first (free), then hosted models
# that have a usable API key.
_FALLBACK_RANK = {"vllm": 0, "ollama": 1}
_HOSTED_RANK = 2

# How long to wait for a TCP connect when probing a self-hosted endpoint. Long
# enough for a LAN box, short enough that a dead host costs a blink.
_PROBE_TIMEOUT_SECONDS = 0.75


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
        Get a model configuration by its key from the repository.

        Args:
            key: The model key to find

        Returns:
            Model configuration if found, None otherwise
        """
        return await self.repository.find_by_key(key)

    async def create_model_config(self, model_data, group_id: Optional[str] = None):
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

        # Invalidate cache - both default and group-specific if provided
        await model_config_cache.invalidate("__default__", "models")
        await model_config_cache.invalidate(
            "__default__", f"model:{model_dict.get('key', model_data.key)}"
        )
        if group_id:
            await model_config_cache.invalidate(group_id, "models")
        logger.info("[CACHE INVALIDATE] Model config cache invalidated after create")

        return result

    async def update_model_config(
        self, key: str, model_data, group_id: Optional[str] = None
    ):
        """
        Update an existing model configuration.

        Args:
            key: Key of the model to update
            model_data: Updated model data
            group_id: Optional group ID for cache invalidation

        Returns:
            Updated model configuration, or None if not found
        """
        # Check if model exists
        existing_model = await self.repository.find_by_key(key)
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

        # Invalidate cache - both default and group-specific if provided
        await model_config_cache.invalidate("__default__", "models")
        await model_config_cache.invalidate("__default__", f"model:{key}")
        if group_id:
            await model_config_cache.invalidate(group_id, "models")
        # Also invalidate the model's own group if it has one
        if existing_model.group_id:
            await model_config_cache.invalidate(existing_model.group_id, "models")
        logger.info("[CACHE INVALIDATE] Model config cache invalidated after update")

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

            # Get the updated model
            result = await self.repository.find_by_key(key)

            # Invalidate cache
            await model_config_cache.invalidate("__default__", "models")
            await model_config_cache.invalidate("__default__", f"model:{key}")
            if group_id:
                await model_config_cache.invalidate(group_id, "models")
            if result and result.group_id:
                await model_config_cache.invalidate(result.group_id, "models")
            logger.info(
                "[CACHE INVALIDATE] Model config cache invalidated after toggle"
            )

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

        # Get the model first to know its group_id for cache invalidation
        existing_model = await self.repository.find_by_key(key)
        model_group_id = existing_model.group_id if existing_model else None

        # Use the dedicated repository method for deletion by key
        result = await self.repository.delete_by_key(key)

        if result:
            # Invalidate cache
            await model_config_cache.invalidate("__default__", "models")
            await model_config_cache.invalidate("__default__", f"model:{key}")
            if group_id:
                await model_config_cache.invalidate(group_id, "models")
            if model_group_id:
                await model_config_cache.invalidate(model_group_id, "models")
            logger.info(
                "[CACHE INVALIDATE] Model config cache invalidated after delete"
            )

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
            # find_by_key is global (not group-filtered), so the cache entry
            # lives under "__default__" and mutations invalidate it exactly.
            # API keys are never cached here — only the model row fields.
            cached = await model_config_cache.get(
                "__default__", f"model:{normalized_key}"
            )
            if cached is not None:
                config = dict(cached)
            else:
                # Try the full model name first (supports HF-style names like
                # "mlx-community/model-name"), then fall back to the normalized
                # key (strips provider prefix e.g. "databricks/model" → "model").
                model_config = await self.repository.find_by_key(model)
                if not model_config:
                    model_config = await self.repository.find_by_key(normalized_key)
                if model_config:
                    config = {
                        "key": model_config.key,
                        "name": model_config.name,
                        "provider": model_config.provider,
                        "temperature": model_config.temperature,
                        "context_window": model_config.context_window,
                        "max_output_tokens": model_config.max_output_tokens,
                        "extended_thinking": model_config.extended_thinking,
                        "enabled": model_config.enabled,
                    }
                else:
                    # Fall back to utility function with normalized key (best-effort; may be None without a sync DB session)
                    config = get_model_config(normalized_key)
                    if not config:
                        raise ValueError(
                            f"Model configuration not found for model: {model}"
                        )
                await model_config_cache.set(
                    "__default__", f"model:{normalized_key}", dict(config)
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

    @staticmethod
    def _as_config(m) -> Dict[str, Any]:
        """Model row -> the config dict shape get_model_config returns."""
        return {
            "key": m.key,
            "name": m.name,
            "provider": m.provider,
            "temperature": m.temperature,
            "context_window": m.context_window,
            "max_output_tokens": m.max_output_tokens,
            "extended_thinking": getattr(m, "extended_thinking", False),
            "enabled": m.enabled,
        }

    async def _fallback_candidate_usable(self, provider: str) -> bool:
        """Can this provider actually serve a request right now?

        Self-hosted: the box must answer a TCP connect. Hosted: an API key must
        exist for this group. Anything that fails here is skipped so the
        substitution lands on a model that works instead of one that merely
        exists in the catalogue.
        """
        if provider in SELF_HOSTED_ENDPOINTS:
            env_var, default = SELF_HOSTED_ENDPOINTS[provider]
            return await _endpoint_reachable(os.getenv(env_var, default))
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

        KASAL_FALLBACK_MODEL pins a specific model key and wins over the ranking
        when that model is enabled.
        """
        try:
            models = await self.find_enabled_models()
        except Exception as e:  # noqa: BLE001 — fallback must never raise
            logger.warning(f"Local-fallback lookup failed: {e}")
            return None

        candidates = [m for m in models if (m.provider or "").lower() != "databricks"]

        pinned_key = os.getenv("KASAL_FALLBACK_MODEL")
        if pinned_key:
            for m in candidates:
                if m.key == pinned_key:
                    logger.info(
                        "Using KASAL_FALLBACK_MODEL pin '%s' for Databricks substitution",
                        pinned_key,
                    )
                    return self._as_config(m)
            logger.warning(
                "KASAL_FALLBACK_MODEL='%s' is not an enabled model — ignoring the pin",
                pinned_key,
            )

        ranked = sorted(
            candidates,
            key=lambda m: _FALLBACK_RANK.get((m.provider or "").lower(), _HOSTED_RANK),
        )
        for m in ranked:
            if await self._fallback_candidate_usable((m.provider or "").lower()):
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
        2. Group-specific models - visible only to members of that group
        3. If a model has both default and group versions, the group version takes precedence

        Args:
            group_context: Group context with group IDs

        Returns:
            List of model configurations for the group
        """
        # Determine cache key based on group context
        cache_group_id = (
            group_context.primary_group_id
            if group_context and group_context.group_ids
            else "__default__"
        )

        # =========================================================================
        # TTL CACHE: Check cache first
        # =========================================================================
        cached_models = await model_config_cache.get(cache_group_id, "models")
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
        if not group_context or not group_context.group_ids:
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

        # Then, override with group-specific models if they exist
        for model in all_models:
            if model.group_id in group_context.group_ids:
                # This will override the default if it exists
                models_by_key[model.key] = model

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
        Toggle the enabled status of a model with group verification.

        For default models (group_id = null):
        - Creates a group-specific copy with the toggled state
        - Ensures each group has their own enabled/disabled settings

        For group-specific models:
        - Only the owning group can toggle them

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
            # First get the model (could be default or group-specific)
            all_models = await self.repository.find_all()

            # Find the model (prefer group-specific over default)
            target_model = None
            default_model = None

            for model in all_models:
                if model.key == key:
                    if model.group_id is None:
                        default_model = model
                    elif group_context and model.group_id in group_context.group_ids:
                        target_model = model
                        break

            # If no group-specific model found, use default
            if not target_model:
                target_model = default_model

            if not target_model:
                logger.warning(f"Model with key {key} not found")
                return None

            # Must have a valid group context to toggle models
            if not group_context or not group_context.group_ids:
                logger.warning(f"No group context provided for toggling model {key}")
                raise ForbiddenError(detail="Group context required to toggle models")

            primary_group_id = group_context.primary_group_id

            # Helper to invalidate cache after mutation
            async def _invalidate_cache():
                await model_config_cache.invalidate(primary_group_id, "models")
                await model_config_cache.invalidate("__default__", "models")
                logger.info(
                    f"[CACHE INVALIDATE] Model config cache invalidated for group {primary_group_id}"
                )

            # If it's a default model (group_id = null), create a group-specific copy
            if target_model.group_id is None:
                # Check if a group-specific version already exists
                existing_group_model = await self.repository.find_by_key_and_group(
                    key, primary_group_id
                )

                if existing_group_model:
                    # Toggle the existing group-specific model in its group scope
                    await self.repository.toggle_enabled_in_group(
                        existing_group_model.key, primary_group_id, enabled
                    )
                    await _invalidate_cache()
                    return await self.repository.find_by_key_and_group(
                        key, primary_group_id
                    )
                else:
                    # Create a new group-specific copy with toggled state
                    model_data = {
                        "key": target_model.key,
                        "name": target_model.name,
                        "provider": (
                            target_model.provider
                            if hasattr(target_model, "provider")
                            else None
                        ),
                        "temperature": (
                            target_model.temperature
                            if hasattr(target_model, "temperature")
                            else None
                        ),
                        "context_window": (
                            target_model.context_window
                            if hasattr(target_model, "context_window")
                            else None
                        ),
                        "max_output_tokens": (
                            target_model.max_output_tokens
                            if hasattr(target_model, "max_output_tokens")
                            else None
                        ),
                        "extended_thinking": (
                            target_model.extended_thinking
                            if hasattr(target_model, "extended_thinking")
                            else False
                        ),
                        "enabled": enabled,  # Use the requested state
                        "group_id": primary_group_id,
                        "created_by_email": group_context.group_email,
                    }
                    result = await self.repository.create(model_data)
                    await _invalidate_cache()
                    return result

            # For group-specific models, check authorization
            if target_model.group_id not in group_context.group_ids:
                logger.warning(f"Model with key {key} not authorized for group")
                raise NotFoundError(detail=f"Model with key {key} not found")

            # Toggle the group-specific model in its group scope
            await self.repository.toggle_enabled_in_group(
                target_model.key, target_model.group_id, enabled
            )
            await _invalidate_cache()
            return await self.repository.find_by_key_and_group(
                key, target_model.group_id
            )

        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle model: {str(e)}")
            raise KasalError(detail=f"Failed to toggle model: {str(e)}")
