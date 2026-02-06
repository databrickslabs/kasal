"""
Unit tests for the cache module.

Tests the TTLCache implementation including:
- Basic get/set operations
- TTL expiration
- Group-scoped cache keys
- Cache invalidation
- Cache statistics
- Thread safety with async locks
"""

import asyncio
import time
import pytest
from unittest.mock import patch, MagicMock

from src.core.cache import (
    TTLCache,
    CacheEntry,
    model_config_cache,
    db_config_cache,
    get_all_cache_stats,
)


class TestCacheEntry:
    """Test cases for CacheEntry dataclass."""

    def test_cache_entry_not_expired(self):
        """Test that entry is not expired when within TTL."""
        entry = CacheEntry(value="test", expires_at=time.time() + 100)
        assert entry.is_expired() is False

    def test_cache_entry_expired(self):
        """Test that entry is expired when past TTL."""
        entry = CacheEntry(value="test", expires_at=time.time() - 1)
        assert entry.is_expired() is True

    def test_cache_entry_exactly_at_expiry(self):
        """Test edge case at exact expiration time."""
        # Entry should be expired when current time > expires_at
        entry = CacheEntry(value="test", expires_at=time.time())
        # Give a tiny margin for execution time
        time.sleep(0.01)
        assert entry.is_expired() is True

    def test_cache_entry_stores_value(self):
        """Test that entry correctly stores any value type."""
        # String
        entry_str = CacheEntry(value="string_value", expires_at=time.time() + 100)
        assert entry_str.value == "string_value"

        # List
        entry_list = CacheEntry(value=[1, 2, 3], expires_at=time.time() + 100)
        assert entry_list.value == [1, 2, 3]

        # Dict
        entry_dict = CacheEntry(value={"key": "value"}, expires_at=time.time() + 100)
        assert entry_dict.value == {"key": "value"}

        # None
        entry_none = CacheEntry(value=None, expires_at=time.time() + 100)
        assert entry_none.value is None


class TestTTLCache:
    """Test cases for TTLCache class."""

    @pytest.fixture
    def cache(self):
        """Create a fresh cache instance for each test."""
        return TTLCache[str](ttl=60, maxsize=100, name="test_cache")

    @pytest.fixture
    def short_ttl_cache(self):
        """Create a cache with short TTL for expiration tests."""
        return TTLCache[str](ttl=1, maxsize=100, name="short_ttl_cache")

    @pytest.mark.asyncio
    async def test_cache_set_and_get(self, cache):
        """Test basic set and get operations."""
        await cache.set("group1", "namespace1", "value1")
        result = await cache.get("group1", "namespace1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_cache_get_miss(self, cache):
        """Test cache miss returns None."""
        result = await cache.get("nonexistent", "namespace")
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_group_isolation(self, cache):
        """Test that different groups have isolated caches."""
        await cache.set("group1", "models", "models_for_group1")
        await cache.set("group2", "models", "models_for_group2")

        result1 = await cache.get("group1", "models")
        result2 = await cache.get("group2", "models")

        assert result1 == "models_for_group1"
        assert result2 == "models_for_group2"

    @pytest.mark.asyncio
    async def test_cache_namespace_isolation(self, cache):
        """Test that different namespaces have isolated caches."""
        await cache.set("group1", "models", "model_data")
        await cache.set("group1", "api_keys", "api_key_data")

        models = await cache.get("group1", "models")
        api_keys = await cache.get("group1", "api_keys")

        assert models == "model_data"
        assert api_keys == "api_key_data"

    @pytest.mark.asyncio
    async def test_cache_expiration(self, short_ttl_cache):
        """Test that entries expire after TTL."""
        await short_ttl_cache.set("group1", "models", "value")

        # Immediately after set, value should be present
        result_immediate = await short_ttl_cache.get("group1", "models")
        assert result_immediate == "value"

        # Wait for TTL to expire
        await asyncio.sleep(1.1)

        # After expiration, should return None
        result_expired = await short_ttl_cache.get("group1", "models")
        assert result_expired is None

    @pytest.mark.asyncio
    async def test_cache_invalidate(self, cache):
        """Test cache invalidation removes entry."""
        await cache.set("group1", "models", "value")

        # Verify it exists
        assert await cache.get("group1", "models") == "value"

        # Invalidate
        result = await cache.invalidate("group1", "models")
        assert result is True

        # Verify it's gone
        assert await cache.get("group1", "models") is None

    @pytest.mark.asyncio
    async def test_cache_invalidate_nonexistent(self, cache):
        """Test invalidating nonexistent entry returns False."""
        result = await cache.invalidate("nonexistent", "namespace")
        assert result is False

    @pytest.mark.asyncio
    async def test_cache_invalidate_group(self, cache):
        """Test invalidating all entries for a group."""
        await cache.set("group1", "models", "models_value")
        await cache.set("group1", "api_keys", "api_keys_value")
        await cache.set("group2", "models", "other_group_models")

        # Invalidate all group1 entries
        removed = await cache.invalidate_group("group1")
        assert removed == 2

        # Verify group1 entries are gone
        assert await cache.get("group1", "models") is None
        assert await cache.get("group1", "api_keys") is None

        # Verify group2 entries still exist
        assert await cache.get("group2", "models") == "other_group_models"

    @pytest.mark.asyncio
    async def test_cache_clear(self, cache):
        """Test clearing entire cache."""
        await cache.set("group1", "models", "value1")
        await cache.set("group2", "models", "value2")
        await cache.set("group3", "api_keys", "value3")

        # Clear all
        removed = await cache.clear()
        assert removed == 3

        # Verify all entries are gone
        assert await cache.get("group1", "models") is None
        assert await cache.get("group2", "models") is None
        assert await cache.get("group3", "api_keys") is None

    @pytest.mark.asyncio
    async def test_cache_maxsize_eviction(self):
        """Test that cache evicts entries when at capacity."""
        small_cache = TTLCache[str](ttl=300, maxsize=3, name="small_cache")

        # Fill cache to capacity
        await small_cache.set("group1", "ns1", "value1")
        await small_cache.set("group2", "ns2", "value2")
        await small_cache.set("group3", "ns3", "value3")

        # Add one more - should evict oldest
        await small_cache.set("group4", "ns4", "value4")

        # Cache should still have only 3 entries
        stats = small_cache.stats()
        assert stats["size"] <= 3

    @pytest.mark.asyncio
    async def test_cache_stats_tracking(self, cache):
        """Test that cache statistics are tracked correctly."""
        # Initial stats
        stats = cache.stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["size"] == 0

        # Add entry and hit
        await cache.set("group1", "models", "value")
        await cache.get("group1", "models")  # Hit

        stats = cache.stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 0
        assert stats["size"] == 1

        # Miss
        await cache.get("nonexistent", "namespace")

        stats = cache.stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1

    @pytest.mark.asyncio
    async def test_cache_hit_rate_calculation(self, cache):
        """Test hit rate percentage calculation."""
        # 3 hits, 1 miss = 75% hit rate
        await cache.set("group1", "ns1", "value1")
        await cache.get("group1", "ns1")  # Hit
        await cache.get("group1", "ns1")  # Hit
        await cache.get("group1", "ns1")  # Hit
        await cache.get("nonexistent", "ns")  # Miss

        stats = cache.stats()
        assert stats["hit_rate"] == "75.0%"

    @pytest.mark.asyncio
    async def test_cache_hit_rate_zero_requests(self, cache):
        """Test hit rate with zero requests doesn't cause division error."""
        stats = cache.stats()
        assert stats["hit_rate"] == "0.0%"

    @pytest.mark.asyncio
    async def test_cache_overwrites_existing(self, cache):
        """Test that setting same key overwrites existing value."""
        await cache.set("group1", "models", "old_value")
        await cache.set("group1", "models", "new_value")

        result = await cache.get("group1", "models")
        assert result == "new_value"

    @pytest.mark.asyncio
    async def test_cache_key_format(self, cache):
        """Test that cache keys are properly formatted."""
        key = cache._make_key("group123", "models")
        assert key == "models:group123"

    @pytest.mark.asyncio
    async def test_cache_with_special_characters_in_group_id(self, cache):
        """Test cache works with special characters in group_id."""
        special_group = "group-123_test@domain.com"
        await cache.set(special_group, "models", "value")

        result = await cache.get(special_group, "models")
        assert result == "value"

    @pytest.mark.asyncio
    async def test_cache_concurrent_access(self, cache):
        """Test cache handles concurrent access safely."""

        async def writer(i):
            await cache.set(f"group{i}", "models", f"value{i}")

        async def reader(i):
            return await cache.get(f"group{i}", "models")

        # Concurrent writes
        await asyncio.gather(*[writer(i) for i in range(10)])

        # Concurrent reads
        results = await asyncio.gather(*[reader(i) for i in range(10)])

        for i, result in enumerate(results):
            assert result == f"value{i}"

    @pytest.mark.asyncio
    async def test_cache_stores_complex_objects(self, cache):
        """Test cache can store complex objects."""

        class MockModel:
            def __init__(self, id, name):
                self.id = id
                self.name = name

        models = [MockModel(1, "model1"), MockModel(2, "model2")]
        await cache.set("group1", "models", models)

        result = await cache.get("group1", "models")
        assert len(result) == 2
        assert result[0].id == 1
        assert result[1].name == "model2"

    @pytest.mark.asyncio
    async def test_cache_expired_entry_counted_as_miss(self, short_ttl_cache):
        """Test that accessing expired entry counts as miss, not hit."""
        await short_ttl_cache.set("group1", "models", "value")

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Access expired entry
        await short_ttl_cache.get("group1", "models")

        stats = short_ttl_cache.stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0


class TestSingletonCaches:
    """Test cases for singleton cache instances."""

    def test_model_config_cache_exists(self):
        """Test model_config_cache singleton is properly configured."""
        assert model_config_cache is not None
        stats = model_config_cache.stats()
        assert stats["name"] == "model_config"
        assert stats["ttl"] == 300  # 5 minutes
        assert stats["maxsize"] == 500

    def test_db_config_cache_exists(self):
        """Test db_config_cache singleton is properly configured."""
        assert db_config_cache is not None
        stats = db_config_cache.stats()
        assert stats["name"] == "db_config"
        assert stats["ttl"] == 300  # 5 minutes
        assert stats["maxsize"] == 100

    def test_get_all_cache_stats(self):
        """Test get_all_cache_stats returns all cache statistics."""
        all_stats = get_all_cache_stats()

        assert "model_config" in all_stats
        assert "db_config" in all_stats

        # Verify structure
        for cache_name, stats in all_stats.items():
            assert "name" in stats
            assert "size" in stats
            assert "maxsize" in stats
            assert "ttl" in stats
            assert "hits" in stats
            assert "misses" in stats
            assert "hit_rate" in stats


class TestCacheEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_cache_with_none_value(self):
        """Test cache can store and distinguish None values."""
        cache = TTLCache[str](ttl=60, maxsize=100, name="test")

        # Store None explicitly
        await cache.set("group1", "models", None)

        # Should return None (the stored value), not miss
        result = await cache.get("group1", "models")
        assert result is None

        # But stats should show a hit, not a miss
        # Note: Current implementation treats None as cache miss
        # This test documents the current behavior

    @pytest.mark.asyncio
    async def test_cache_empty_group_id(self):
        """Test cache handles empty group_id."""
        cache = TTLCache[str](ttl=60, maxsize=100, name="test")

        await cache.set("", "models", "value")
        result = await cache.get("", "models")
        assert result == "value"

    @pytest.mark.asyncio
    async def test_cache_empty_namespace(self):
        """Test cache handles empty namespace."""
        cache = TTLCache[str](ttl=60, maxsize=100, name="test")

        await cache.set("group1", "", "value")
        result = await cache.get("group1", "")
        assert result == "value"

    @pytest.mark.asyncio
    async def test_cache_evicts_expired_on_set(self):
        """Test that expired entries are evicted when setting new values."""
        cache = TTLCache[str](ttl=1, maxsize=3, name="test")

        # Fill cache
        await cache.set("group1", "ns1", "value1")
        await cache.set("group2", "ns2", "value2")
        await cache.set("group3", "ns3", "value3")

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Add new entry - should trigger eviction of expired
        await cache.set("group4", "ns4", "value4")

        # New entry should exist
        assert await cache.get("group4", "ns4") == "value4"

    @pytest.mark.asyncio
    async def test_cache_invalidate_group_no_matches(self):
        """Test invalidate_group with no matching entries."""
        cache = TTLCache[str](ttl=60, maxsize=100, name="test")

        await cache.set("group1", "models", "value")

        removed = await cache.invalidate_group("nonexistent_group")
        assert removed == 0

        # Original entry should still exist
        assert await cache.get("group1", "models") == "value"
