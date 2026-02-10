"""
Unit tests for ModelConfigService caching behavior.

Tests the TTL cache integration including:
- Cache hit/miss scenarios for find_all_for_group
- Cache invalidation on create, update, delete, toggle operations
- Group-scoped cache key isolation
- Bulk operation cache clearing
"""

import pytest
import pytest_asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.model_config_service import ModelConfigService
from src.core.cache import model_config_cache, TTLCache
from src.utils.user_context import GroupContext


def mk_model(key="k", name="N", provider="openai", enabled=True, group_id=None,
             temperature=0.1, context_window=8192, max_output_tokens=2048, extended_thinking=False):
    """Create a mock model configuration."""
    return SimpleNamespace(
        key=key, name=name, provider=provider, enabled=enabled, group_id=group_id,
        temperature=temperature, context_window=context_window,
        max_output_tokens=max_output_tokens, extended_thinking=extended_thinking,
        id=1
    )


def make_group_context(group_ids=None, email="user@test.com"):
    """Create a GroupContext for testing."""
    if group_ids is None:
        group_ids = ["group1"]
    gc = GroupContext()
    gc.group_ids = group_ids
    gc.group_email = email
    gc.email_domain = "test.com"
    gc.user_role = "admin"
    return gc


@pytest.fixture
def fresh_cache():
    """Create a fresh cache instance for each test to avoid state pollution."""
    cache = TTLCache[list](ttl=300, maxsize=500, name="test_model_config")
    return cache


@pytest_asyncio.fixture
async def clean_model_cache():
    """Clean the global model_config_cache before and after each test."""
    await model_config_cache.clear()
    yield model_config_cache
    await model_config_cache.clear()


class TestFindAllForGroupCaching:
    """Test cases for find_all_for_group caching behavior."""

    @pytest.mark.asyncio
    async def test_cache_miss_fetches_from_database(self, clean_model_cache):
        """Test that cache miss triggers database fetch."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1"), mk_model("m2")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])
        result = await svc.find_all_for_group(gc)

        # Database should be called on cache miss
        repo.find_all.assert_called_once()
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached_data(self, clean_model_cache):
        """Test that cache hit returns cached data without database call."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1"), mk_model("m2")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # First call - cache miss
        result1 = await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Second call - cache hit (should not call database again)
        result2 = await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1  # Still 1, not 2

        # Results should be the same
        assert result1 == result2

    @pytest.mark.asyncio
    async def test_different_groups_have_separate_cache_entries(self, clean_model_cache):
        """Test that different groups have isolated cache entries."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1"), mk_model("m2")]
        repo.find_all = AsyncMock(return_value=models)

        gc1 = make_group_context(group_ids=["group1"])
        gc2 = make_group_context(group_ids=["group2"])

        # First call for group1
        await svc.find_all_for_group(gc1)
        assert repo.find_all.call_count == 1

        # Second call for group2 (different group, should miss cache)
        await svc.find_all_for_group(gc2)
        assert repo.find_all.call_count == 2

        # Third call for group1 (should hit cache)
        await svc.find_all_for_group(gc1)
        assert repo.find_all.call_count == 2  # Still 2

    @pytest.mark.asyncio
    async def test_no_group_context_uses_default_cache_key(self, clean_model_cache):
        """Test that empty group context uses __default__ cache key."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1", group_id=None)]
        repo.find_all = AsyncMock(return_value=models)

        # Empty group context
        gc = GroupContext()

        result = await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Second call should hit cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1  # Cache hit

    @pytest.mark.asyncio
    async def test_cache_stores_filtered_models_for_group(self, clean_model_cache):
        """Test that cache stores the correctly filtered models for each group."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        # Mix of default and group-specific models
        models = [
            mk_model("default1", group_id=None),
            mk_model("default2", group_id=None),
            mk_model("group1_model", group_id="group1"),
            mk_model("group2_model", group_id="group2"),
        ]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])
        result = await svc.find_all_for_group(gc)

        # Should return default models + group1 models, not group2 models
        result_keys = [m.key for m in result]
        assert "default1" in result_keys
        assert "default2" in result_keys
        assert "group1_model" in result_keys
        assert "group2_model" not in result_keys


class TestCacheInvalidationOnMutations:
    """Test cases for cache invalidation during mutations."""

    @pytest.mark.asyncio
    async def test_create_model_invalidates_cache(self, clean_model_cache):
        """Test that creating a model invalidates the cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        # Setup for find_all_for_group
        models = [mk_model("m1")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Create a new model (should invalidate cache)
        repo.find_by_key = AsyncMock(return_value=None)
        repo.create = AsyncMock(return_value=mk_model("new_model"))

        class NewModel:
            key = "new_model"
            def model_dump(self):
                return {"key": self.key}

        await svc.create_model_config(NewModel(), group_id="group1")

        # Now find_all_for_group should miss cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 2  # Called again after invalidation

    @pytest.mark.asyncio
    async def test_update_model_invalidates_cache(self, clean_model_cache):
        """Test that updating a model invalidates the cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Update model (should invalidate cache)
        repo.find_by_key = AsyncMock(return_value=mk_model("m1"))
        repo.update = AsyncMock(return_value=mk_model("m1", name="Updated"))

        await svc.update_model_config("m1", {"name": "Updated"}, group_id="group1")

        # Now find_all_for_group should miss cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 2

    @pytest.mark.asyncio
    async def test_delete_model_invalidates_cache(self, clean_model_cache):
        """Test that deleting a model invalidates the cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1"), mk_model("m2")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Delete model (should invalidate cache)
        repo.find_by_key = AsyncMock(return_value=mk_model("m1"))
        repo.delete_by_key = AsyncMock(return_value=True)

        await svc.delete_model_config("m1", group_id="group1")

        # Now find_all_for_group should miss cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 2

    @pytest.mark.asyncio
    async def test_toggle_model_invalidates_cache(self, clean_model_cache):
        """Test that toggling model enabled status invalidates the cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1", enabled=True)]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Toggle model (should invalidate cache)
        repo.toggle_enabled = AsyncMock(return_value=True)
        repo.find_by_key = AsyncMock(return_value=mk_model("m1", enabled=False))

        await svc.toggle_model_enabled("m1", False, group_id="group1")

        # Now find_all_for_group should miss cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 2

    @pytest.mark.asyncio
    async def test_enable_all_models_clears_entire_cache(self, clean_model_cache):
        """Test that enable_all_models clears the entire cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1"), mk_model("m2")]
        repo.find_all = AsyncMock(return_value=models)

        # Populate cache for multiple groups
        gc1 = make_group_context(group_ids=["group1"])
        gc2 = make_group_context(group_ids=["group2"])

        await svc.find_all_for_group(gc1)
        await svc.find_all_for_group(gc2)
        initial_call_count = repo.find_all.call_count

        # Enable all models (should clear entire cache)
        repo.enable_all_models = AsyncMock(return_value=True)
        await svc.enable_all_models()

        # Both groups should now miss cache
        await svc.find_all_for_group(gc1)
        await svc.find_all_for_group(gc2)
        # Each should trigger a new database call
        assert repo.find_all.call_count == initial_call_count + 3  # +1 for enable_all, +2 for find_all_for_group

    @pytest.mark.asyncio
    async def test_disable_all_models_clears_entire_cache(self, clean_model_cache):
        """Test that disable_all_models clears the entire cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1"), mk_model("m2")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Disable all models (should clear cache)
        repo.disable_all_models = AsyncMock(return_value=True)
        await svc.disable_all_models()

        # Should miss cache now
        await svc.find_all_for_group(gc)
        # +1 for disable_all find_all, +1 for find_all_for_group
        assert repo.find_all.call_count == 3

    @pytest.mark.asyncio
    async def test_toggle_global_enabled_clears_entire_cache(self, clean_model_cache):
        """Test that toggle_global_enabled clears the entire cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1", group_id=None)]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 1

        # Toggle global enabled (should clear cache)
        repo.toggle_global_enabled = AsyncMock(return_value=True)
        repo.find_global_by_key = AsyncMock(return_value=mk_model("m1", enabled=False))

        await svc.toggle_global_enabled("m1", False)

        # Should miss cache now
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == 2


class TestToggleModelEnabledWithGroupCaching:
    """Test cases for toggle_model_enabled_with_group cache invalidation."""

    @pytest.mark.asyncio
    async def test_toggle_with_group_invalidates_group_cache(self, clean_model_cache):
        """Test that toggle_model_enabled_with_group invalidates group cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        # Default model
        models = [mk_model("m1", group_id=None, enabled=True)]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        initial_count = repo.find_all.call_count

        # Toggle with group (creates group-specific copy)
        repo.find_by_key_and_group = AsyncMock(return_value=None)
        repo.create = AsyncMock(return_value=mk_model("m1", group_id="group1", enabled=False))

        await svc.toggle_model_enabled_with_group("m1", False, gc)

        # Cache should be invalidated
        # Note: toggle_model_enabled_with_group internally calls find_all (+1)
        # Then our find_all_for_group call also triggers find_all (+1)
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == initial_count + 2

    @pytest.mark.asyncio
    async def test_toggle_existing_group_override_invalidates_cache(self, clean_model_cache):
        """Test toggling existing group override invalidates cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1", group_id=None, enabled=True)]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        # Populate cache
        await svc.find_all_for_group(gc)
        initial_count = repo.find_all.call_count

        # Toggle existing group override
        repo.find_by_key_and_group = AsyncMock(return_value=mk_model("m1", group_id="group1", enabled=True))
        repo.toggle_enabled_in_group = AsyncMock(return_value=True)

        await svc.toggle_model_enabled_with_group("m1", False, gc)

        # Cache should be invalidated
        # Note: toggle_model_enabled_with_group internally calls find_all (+1)
        # Then our find_all_for_group call also triggers find_all (+1)
        await svc.find_all_for_group(gc)
        assert repo.find_all.call_count == initial_count + 2


class TestCacheInvalidationWithModelGroupId:
    """Test cache invalidation when model has its own group_id."""

    @pytest.mark.asyncio
    async def test_update_invalidates_models_own_group_cache(self, clean_model_cache):
        """Test that updating a model also invalidates its own group's cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        # Model belongs to group2
        model_with_group = mk_model("m1", group_id="group2")
        repo.find_by_key = AsyncMock(return_value=model_with_group)
        repo.update = AsyncMock(return_value=model_with_group)

        # Populate cache for group2
        repo.find_all = AsyncMock(return_value=[model_with_group])
        gc2 = make_group_context(group_ids=["group2"])
        await svc.find_all_for_group(gc2)
        initial_count = repo.find_all.call_count

        # Update with group1 context - should invalidate both group1 and model's group2
        await svc.update_model_config("m1", {"name": "Updated"}, group_id="group1")

        # group2's cache should also be invalidated
        await svc.find_all_for_group(gc2)
        assert repo.find_all.call_count == initial_count + 1

    @pytest.mark.asyncio
    async def test_delete_invalidates_models_own_group_cache(self, clean_model_cache):
        """Test that deleting a model invalidates its own group's cache."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        # Model belongs to group2
        model_with_group = mk_model("m1", group_id="group2")
        repo.find_by_key = AsyncMock(return_value=model_with_group)
        repo.delete_by_key = AsyncMock(return_value=True)

        # Populate cache for group2
        repo.find_all = AsyncMock(return_value=[model_with_group])
        gc2 = make_group_context(group_ids=["group2"])
        await svc.find_all_for_group(gc2)
        initial_count = repo.find_all.call_count

        # Delete with group1 context
        await svc.delete_model_config("m1", group_id="group1")

        # group2's cache should also be invalidated
        await svc.find_all_for_group(gc2)
        assert repo.find_all.call_count == initial_count + 1


class TestCacheStatsIntegration:
    """Test that cache operations properly update statistics."""

    @pytest.mark.asyncio
    async def test_cache_hit_increments_hit_counter(self, clean_model_cache):
        """Test that cache hits are properly tracked."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["group1"])

        initial_stats = model_config_cache.stats()
        initial_hits = initial_stats["hits"]

        # First call - cache miss
        await svc.find_all_for_group(gc)

        # Second call - cache hit
        await svc.find_all_for_group(gc)

        final_stats = model_config_cache.stats()
        assert final_stats["hits"] == initial_hits + 1

    @pytest.mark.asyncio
    async def test_cache_miss_increments_miss_counter(self, clean_model_cache):
        """Test that cache misses are properly tracked."""
        svc = ModelConfigService(session=SimpleNamespace())
        repo = svc.repository = AsyncMock()

        models = [mk_model("m1")]
        repo.find_all = AsyncMock(return_value=models)

        gc = make_group_context(group_ids=["unique_group_for_miss_test"])

        initial_stats = model_config_cache.stats()
        initial_misses = initial_stats["misses"]

        # First call - cache miss
        await svc.find_all_for_group(gc)

        final_stats = model_config_cache.stats()
        assert final_stats["misses"] == initial_misses + 1
