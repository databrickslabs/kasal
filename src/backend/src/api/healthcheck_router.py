from fastapi import APIRouter

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