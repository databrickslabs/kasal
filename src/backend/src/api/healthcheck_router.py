import logging

from fastapi import APIRouter
from sqlalchemy import text

from src.core.cache import get_all_cache_stats

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/health",
    tags=["health"],
)


@router.get("")
async def health_check():
    """
    Health check endpoint to verify API is running.

    Returns:
        dict: Status information
    """
    return {
        "status": "ok",
        "message": "Service is healthy",
    }


@router.get("/db")
async def db_health():
    """
    Database health check — reports Lakebase reachability when enabled.

    Returns:
        dict: Lakebase status including enabled, activated, reachable, and error info
    """
    from src.db.database_router import is_lakebase_enabled
    from src.db.lakebase_state import (
        get_last_successful_connection,
        is_lakebase_activated,
    )

    lakebase_enabled = await is_lakebase_enabled()
    result = {
        "status": "ok",
        "lakebase_enabled": lakebase_enabled,
        "lakebase_activated": is_lakebase_activated(),
        "lakebase_reachable": None,
        "lakebase_error": None,
    }

    if lakebase_enabled:
        try:
            from src.db.session import async_session_factory

            if async_session_factory.is_lakebase:
                async with async_session_factory() as session:
                    await session.execute(text("SELECT 1"))
                result["lakebase_reachable"] = True
            else:
                result["lakebase_reachable"] = False
                result["lakebase_error"] = "Lakebase factory not initialised"
        except Exception as e:
            result["lakebase_reachable"] = False
            result["lakebase_error"] = str(e)
            result["status"] = "degraded"

    last_conn = get_last_successful_connection()
    if last_conn:
        result["last_successful_connection"] = last_conn.isoformat()

    return result


@router.get("/cache")
async def cache_stats():
    """
    Get cache statistics for monitoring performance.

    Returns cache hit/miss rates and sizes for:
    - model_config: Model configuration cache (5 min TTL)
    - db_config: Database configuration cache (5 min TTL)

    Returns:
        dict: Cache statistics including hit rates and sizes
    """
    return {
        "status": "ok",
        "caches": get_all_cache_stats(),
    }
