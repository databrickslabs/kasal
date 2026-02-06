from fastapi import APIRouter

from src.core.cache import get_all_cache_stats

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