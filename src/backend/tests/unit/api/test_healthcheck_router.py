"""
Unit tests for the healthcheck router.

Tests the health check endpoints including:
- Basic health check endpoint
- Cache statistics endpoint
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, AsyncMock

from src.api.healthcheck_router import health_check, cache_stats, db_health


class TestHealthCheckEndpoint:
    """Test cases for the basic health check endpoint."""

    @pytest.mark.asyncio
    async def test_health_check_returns_ok_status(self):
        """Test that health check returns ok status."""
        result = await health_check()

        assert result["status"] == "ok"
        assert result["message"] == "Service is healthy"

    @pytest.mark.asyncio
    async def test_health_check_response_structure(self):
        """Test that health check has correct response structure."""
        result = await health_check()

        assert "status" in result
        assert "message" in result
        assert len(result) == 2


class TestCacheStatsEndpoint:
    """Test cases for the cache statistics endpoint."""

    @pytest.mark.asyncio
    async def test_cache_stats_returns_ok_status(self):
        """Test that cache stats returns ok status."""
        result = await cache_stats()

        assert result["status"] == "ok"
        assert "caches" in result

    @pytest.mark.asyncio
    async def test_cache_stats_contains_model_config_cache(self):
        """Test that cache stats includes model_config cache."""
        result = await cache_stats()

        assert "model_config" in result["caches"]
        model_config_stats = result["caches"]["model_config"]

        # Verify expected fields are present
        assert "name" in model_config_stats
        assert "size" in model_config_stats
        assert "maxsize" in model_config_stats
        assert "ttl" in model_config_stats
        assert "hits" in model_config_stats
        assert "misses" in model_config_stats
        assert "hit_rate" in model_config_stats

    @pytest.mark.asyncio
    async def test_cache_stats_contains_db_config_cache(self):
        """Test that cache stats includes db_config cache."""
        result = await cache_stats()

        assert "db_config" in result["caches"]
        db_config_stats = result["caches"]["db_config"]

        # Verify expected fields are present
        assert "name" in db_config_stats
        assert "size" in db_config_stats
        assert "maxsize" in db_config_stats
        assert "ttl" in db_config_stats
        assert "hits" in db_config_stats
        assert "misses" in db_config_stats
        assert "hit_rate" in db_config_stats

    @pytest.mark.asyncio
    async def test_cache_stats_model_config_has_correct_settings(self):
        """Test that model_config cache has correct configuration."""
        result = await cache_stats()

        model_config_stats = result["caches"]["model_config"]

        assert model_config_stats["name"] == "model_config"
        assert model_config_stats["ttl"] == 300  # 5 minutes
        assert model_config_stats["maxsize"] == 500

    @pytest.mark.asyncio
    async def test_cache_stats_db_config_has_correct_settings(self):
        """Test that db_config cache has correct configuration."""
        result = await cache_stats()

        db_config_stats = result["caches"]["db_config"]

        assert db_config_stats["name"] == "db_config"
        assert db_config_stats["ttl"] == 300  # 5 minutes
        assert db_config_stats["maxsize"] == 100

    @pytest.mark.asyncio
    async def test_cache_stats_hit_rate_format(self):
        """Test that hit rate is formatted as percentage string."""
        result = await cache_stats()

        for cache_name, stats in result["caches"].items():
            hit_rate = stats["hit_rate"]
            assert isinstance(hit_rate, str)
            assert hit_rate.endswith("%")

    @pytest.mark.asyncio
    async def test_cache_stats_numeric_fields_are_integers(self):
        """Test that numeric fields are integers."""
        result = await cache_stats()

        for cache_name, stats in result["caches"].items():
            assert isinstance(stats["size"], int)
            assert isinstance(stats["maxsize"], int)
            assert isinstance(stats["ttl"], int)
            assert isinstance(stats["hits"], int)
            assert isinstance(stats["misses"], int)

    @pytest.mark.asyncio
    @patch('src.api.healthcheck_router.get_all_cache_stats')
    async def test_cache_stats_uses_get_all_cache_stats(self, mock_get_stats):
        """Test that cache_stats endpoint uses the get_all_cache_stats function."""
        mock_get_stats.return_value = {
            "test_cache": {
                "name": "test_cache",
                "size": 10,
                "maxsize": 100,
                "ttl": 60,
                "hits": 50,
                "misses": 5,
                "hit_rate": "90.9%"
            }
        }

        result = await cache_stats()

        mock_get_stats.assert_called_once()
        assert result["caches"]["test_cache"]["name"] == "test_cache"
        assert result["caches"]["test_cache"]["hits"] == 50

    @pytest.mark.asyncio
    @patch('src.api.healthcheck_router.get_all_cache_stats')
    async def test_cache_stats_handles_empty_caches(self, mock_get_stats):
        """Test that cache_stats handles empty cache stats."""
        mock_get_stats.return_value = {}

        result = await cache_stats()

        assert result["status"] == "ok"
        assert result["caches"] == {}

    @pytest.mark.asyncio
    @patch('src.api.healthcheck_router.get_all_cache_stats')
    async def test_cache_stats_handles_high_hit_rate(self, mock_get_stats):
        """Test cache stats with high hit rate."""
        mock_get_stats.return_value = {
            "model_config": {
                "name": "model_config",
                "size": 50,
                "maxsize": 500,
                "ttl": 300,
                "hits": 1000,
                "misses": 1,
                "hit_rate": "99.9%"
            }
        }

        result = await cache_stats()

        assert result["caches"]["model_config"]["hits"] == 1000
        assert result["caches"]["model_config"]["hit_rate"] == "99.9%"

    @pytest.mark.asyncio
    @patch('src.api.healthcheck_router.get_all_cache_stats')
    async def test_cache_stats_handles_zero_requests(self, mock_get_stats):
        """Test cache stats when no requests have been made."""
        mock_get_stats.return_value = {
            "model_config": {
                "name": "model_config",
                "size": 0,
                "maxsize": 500,
                "ttl": 300,
                "hits": 0,
                "misses": 0,
                "hit_rate": "0.0%"
            }
        }

        result = await cache_stats()

        assert result["caches"]["model_config"]["hits"] == 0
        assert result["caches"]["model_config"]["misses"] == 0
        assert result["caches"]["model_config"]["hit_rate"] == "0.0%"


# ---------------------------------------------------------------------------
# /health/db — Database health endpoint
# ---------------------------------------------------------------------------

class TestDbHealthEndpoint:
    """Test cases for the database health check endpoint."""

    @pytest.mark.asyncio
    @patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False)
    @patch("src.db.lakebase_state.is_lakebase_activated", return_value=False)
    @patch("src.db.lakebase_state.get_last_successful_connection", return_value=None)
    async def test_lakebase_disabled(self, mock_last_conn, mock_activated, mock_enabled):
        """When Lakebase is disabled, return basic ok status."""
        result = await db_health()

        assert result["status"] == "ok"
        assert result["lakebase_enabled"] is False
        assert result["lakebase_activated"] is False
        assert result["lakebase_reachable"] is None
        assert result["lakebase_error"] is None

    @pytest.mark.asyncio
    @patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=True)
    @patch("src.db.lakebase_state.is_lakebase_activated", return_value=True)
    @patch("src.db.lakebase_state.get_last_successful_connection", return_value=None)
    async def test_lakebase_enabled_and_reachable(self, mock_last_conn, mock_activated, mock_enabled):
        """When Lakebase is enabled and reachable, report reachable=True."""
        mock_session = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.is_lakebase = True
        mock_factory.return_value = AsyncMock()
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch("src.db.session.async_session_factory", mock_factory):
            result = await db_health()

        assert result["status"] == "ok"
        assert result["lakebase_enabled"] is True
        assert result["lakebase_activated"] is True
        assert result["lakebase_reachable"] is True

    @pytest.mark.asyncio
    @patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=True)
    @patch("src.db.lakebase_state.is_lakebase_activated", return_value=True)
    @patch("src.db.lakebase_state.get_last_successful_connection", return_value=None)
    async def test_lakebase_enabled_but_unreachable(self, mock_last_conn, mock_activated, mock_enabled):
        """When Lakebase is enabled but SELECT 1 fails, report degraded."""
        mock_factory = MagicMock()
        mock_factory.is_lakebase = True
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(side_effect=ConnectionError("connection refused"))
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_factory.return_value = mock_ctx

        with patch("src.db.session.async_session_factory", mock_factory):
            result = await db_health()

        assert result["status"] == "degraded"
        assert result["lakebase_reachable"] is False
        assert "connection refused" in result["lakebase_error"]

    @pytest.mark.asyncio
    @patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=True)
    @patch("src.db.lakebase_state.is_lakebase_activated", return_value=False)
    @patch("src.db.lakebase_state.get_last_successful_connection", return_value=None)
    async def test_lakebase_enabled_factory_not_initialised(self, mock_last_conn, mock_activated, mock_enabled):
        """When Lakebase is enabled but factory not swapped, report not initialised."""
        mock_factory = MagicMock()
        mock_factory.is_lakebase = False

        with patch("src.db.session.async_session_factory", mock_factory):
            result = await db_health()

        assert result["lakebase_reachable"] is False
        assert result["lakebase_error"] == "Lakebase factory not initialised"

    @pytest.mark.asyncio
    @patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False)
    @patch("src.db.lakebase_state.is_lakebase_activated", return_value=True)
    @patch("src.db.lakebase_state.get_last_successful_connection")
    async def test_includes_last_successful_connection(self, mock_last_conn, mock_activated, mock_enabled):
        """When last_successful_connection is set, include it in the response."""
        ts = datetime(2026, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
        mock_last_conn.return_value = ts

        result = await db_health()

        assert result["last_successful_connection"] == ts.isoformat()

    @pytest.mark.asyncio
    @patch("src.db.database_router.is_lakebase_enabled", new_callable=AsyncMock, return_value=False)
    @patch("src.db.lakebase_state.is_lakebase_activated", return_value=False)
    @patch("src.db.lakebase_state.get_last_successful_connection", return_value=None)
    async def test_no_last_connection_key_when_none(self, mock_last_conn, mock_activated, mock_enabled):
        """When no successful connection recorded, omit last_successful_connection."""
        result = await db_health()

        assert "last_successful_connection" not in result
