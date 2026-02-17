"""
Caching utilities for performance optimization.

This module provides in-memory caching with TTL (Time-To-Live) support
for reducing database queries on frequently accessed, rarely changed data.

Security Considerations:
- All caches are group-scoped to maintain multi-tenant isolation
- Cache keys always include group_id to prevent cross-tenant data leakage
- TTL ensures stale data is automatically evicted
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional, TypeVar, Generic
from dataclasses import dataclass

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """A cached value with expiration timestamp."""

    value: T
    expires_at: float

    def is_expired(self) -> bool:
        """Check if this entry has expired."""
        return time.time() > self.expires_at


class TTLCache(Generic[T]):
    """
    Simple in-memory TTL cache with group-scoped keys.

    Thread-safe for async operations using asyncio.Lock.
    Automatically evicts expired entries on access.

    Example:
        cache = TTLCache[List[ModelConfig]](ttl=300, maxsize=1000)

        # Get or set
        models = await cache.get("group123", "models")
        if models is None:
            models = await fetch_from_db()
            await cache.set("group123", "models", models)

        # Invalidate on mutation
        await cache.invalidate("group123", "models")
    """

    def __init__(self, ttl: int = 300, maxsize: int = 1000, name: str = "cache"):
        """
        Initialize the cache.

        Args:
            ttl: Time-to-live in seconds (default 5 minutes)
            maxsize: Maximum number of entries (default 1000)
            name: Cache name for logging
        """
        self._cache: Dict[str, CacheEntry[T]] = {}
        self._ttl = ttl
        self._maxsize = maxsize
        self._name = name
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    def _make_key(self, group_id: str, namespace: str) -> str:
        """
        Create a cache key from group_id and namespace.

        SECURITY: Always includes group_id for tenant isolation.
        """
        return f"{namespace}:{group_id}"

    async def get(self, group_id: str, namespace: str) -> Optional[T]:
        """
        Get a value from the cache.

        Args:
            group_id: Group ID for tenant isolation
            namespace: Cache namespace (e.g., "models", "api_keys")

        Returns:
            Cached value if found and not expired, None otherwise
        """
        key = self._make_key(group_id, namespace)

        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired():
                del self._cache[key]
                self._misses += 1
                logger.debug(f"[{self._name}] Cache miss (expired): {key}")
                return None

            self._hits += 1
            logger.debug(f"[{self._name}] Cache hit: {key}")
            return entry.value

    async def set(self, group_id: str, namespace: str, value: T) -> None:
        """
        Set a value in the cache.

        Args:
            group_id: Group ID for tenant isolation
            namespace: Cache namespace
            value: Value to cache
        """
        key = self._make_key(group_id, namespace)

        async with self._lock:
            # Evict oldest entries if at capacity
            if len(self._cache) >= self._maxsize:
                await self._evict_expired_unsafe()

                # If still at capacity, remove oldest entry
                if len(self._cache) >= self._maxsize:
                    oldest_key = next(iter(self._cache))
                    del self._cache[oldest_key]
                    logger.debug(f"[{self._name}] Evicted oldest: {oldest_key}")

            expires_at = time.time() + self._ttl
            self._cache[key] = CacheEntry(value=value, expires_at=expires_at)
            logger.debug(f"[{self._name}] Cache set: {key} (expires in {self._ttl}s)")

    async def invalidate(self, group_id: str, namespace: str) -> bool:
        """
        Invalidate (remove) a specific cache entry.

        Args:
            group_id: Group ID for tenant isolation
            namespace: Cache namespace

        Returns:
            True if entry was removed, False if not found
        """
        key = self._make_key(group_id, namespace)

        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.info(f"[{self._name}] Cache invalidated: {key}")
                return True
            return False

    async def invalidate_group(self, group_id: str) -> int:
        """
        Invalidate all cache entries for a group.

        Args:
            group_id: Group ID to invalidate

        Returns:
            Number of entries removed
        """
        suffix = f":{group_id}"
        removed = 0

        async with self._lock:
            keys_to_remove = [k for k in self._cache.keys() if k.endswith(suffix)]
            for key in keys_to_remove:
                del self._cache[key]
                removed += 1

            if removed > 0:
                logger.info(
                    f"[{self._name}] Invalidated {removed} entries for group {group_id}"
                )

        return removed

    async def clear(self) -> int:
        """
        Clear all cache entries.

        Returns:
            Number of entries removed
        """
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"[{self._name}] Cache cleared: {count} entries")
            return count

    async def _evict_expired_unsafe(self) -> int:
        """
        Remove expired entries (must be called with lock held).

        Returns:
            Number of entries removed
        """
        now = time.time()
        expired_keys = [k for k, v in self._cache.items() if v.expires_at < now]

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            logger.debug(f"[{self._name}] Evicted {len(expired_keys)} expired entries")

        return len(expired_keys)

    def stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with hit/miss counts and size
        """
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.0

        return {
            "name": self._name,
            "size": len(self._cache),
            "maxsize": self._maxsize,
            "ttl": self._ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{hit_rate:.1f}%",
        }


# =============================================================================
# Singleton Cache Instances
# =============================================================================

# Model configs cache - 5 minute TTL (models rarely change)
model_config_cache: TTLCache = TTLCache(ttl=300, maxsize=500, name="model_config")

# Database configs cache - 5 minute TTL
db_config_cache: TTLCache = TTLCache(ttl=300, maxsize=100, name="db_config")

# Intent detection cache - 2 minute TTL (short because user context evolves)
intent_cache: TTLCache = TTLCache(ttl=120, maxsize=500, name="intent")


def get_all_cache_stats() -> Dict[str, Dict[str, Any]]:
    """
    Get statistics for all cache instances.

    Returns:
        Dictionary mapping cache names to their statistics
    """
    return {
        "model_config": model_config_cache.stats(),
        "db_config": db_config_cache.stats(),
        "intent": intent_cache.stats(),
    }
