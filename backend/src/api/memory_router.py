"""
API router for memory management operations.

This module defines the FastAPI router for managing CrewAI agent memories,
including listing, resetting, deleting, and searching memories.
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks

from src.engines.crewai.memory_config import MemoryConfig
from src.schemas.memory import (
    MemoryListResponse,
    MemoryActionResponse,
    MemorySearchResponse,
    MemorySearchItem,
    MemoryDetailsResponse,
    MemoryStatsResponse,
    MemoryCleanupResponse,
)

# Configure logging
logger = logging.getLogger(__name__)

# Create router
memory_router = APIRouter(
    prefix="/memory",
    tags=["Memory Management"],
    responses={404: {"description": "Memory not found"}},
)

# Alias for consistent imports
router = memory_router


@memory_router.get("/list", response_model=MemoryListResponse)
async def list_memories():
    """
    List all crew memories.
    
    Returns:
        MemoryListResponse: List of crew names with memory storage and count
    """
    try:
        memories = MemoryConfig.list_crew_memories()
        logger.info(f"Listed {len(memories)} crew memories")
        return MemoryListResponse(memories=memories, count=len(memories))
    except Exception as e:
        logger.error(f"Error listing memories: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list memories: {str(e)}"
        )


@memory_router.post("/reset/{crew_name}", response_model=MemoryActionResponse)
async def reset_memory(crew_name: str):
    """
    Reset memory for a specific crew.
    
    Args:
        crew_name: Name of the crew to reset memory for
        
    Returns:
        MemoryActionResponse: Result of the reset operation
    """
    try:
        success = MemoryConfig.reset_crew_memory(crew_name)
        if success:
            logger.info(f"Memory reset for crew '{crew_name}'")
            return MemoryActionResponse(
                status="success",
                message=f"Memory reset for crew '{crew_name}'"
            )
        else:
            logger.warning(f"Failed to reset memory for crew '{crew_name}' - memory not found")
            raise HTTPException(
                status_code=404,
                detail=f"No memory found for crew '{crew_name}'"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting memory for crew '{crew_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset memory: {str(e)}"
        )


@memory_router.delete("/delete/{crew_name}", response_model=MemoryActionResponse)
async def delete_memory(crew_name: str):
    """
    Delete memory for a specific crew.
    
    Args:
        crew_name: Name of the crew to delete memory for
        
    Returns:
        MemoryActionResponse: Result of the delete operation
    """
    try:
        success = MemoryConfig.delete_crew_memory(crew_name)
        if success:
            logger.info(f"Memory deleted for crew '{crew_name}'")
            return MemoryActionResponse(
                status="success",
                message=f"Memory deleted for crew '{crew_name}'"
            )
        else:
            logger.warning(f"Failed to delete memory for crew '{crew_name}' - memory not found")
            raise HTTPException(
                status_code=404,
                detail=f"No memory found for crew '{crew_name}'"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting memory for crew '{crew_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete memory: {str(e)}"
        )


@memory_router.post("/remove/{crew_name}", response_model=MemoryActionResponse)
async def remove_memory(crew_name: str):
    """
    Alternative endpoint to delete memory using POST method.
    
    This endpoint exists for clients that cannot use DELETE HTTP method.
    
    Args:
        crew_name: Name of the crew to delete memory for
        
    Returns:
        MemoryActionResponse: Result of the delete operation
    """
    return await delete_memory(crew_name)


@memory_router.post("/reset-all", response_model=MemoryActionResponse)
async def reset_all_memories():
    """
    Reset all crew memories.
    
    Returns:
        MemoryActionResponse: Result of the reset operation
    """
    try:
        success = MemoryConfig.reset_all_memories()
        if success:
            logger.info("All memories reset successfully")
            return MemoryActionResponse(
                status="success",
                message="All memories reset successfully"
            )
        else:
            logger.warning("Failed to reset all memories")
            raise HTTPException(
                status_code=500,
                detail="Failed to reset all memories"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting all memories: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset all memories: {str(e)}"
        )


@memory_router.get("/details/{crew_name}", response_model=MemoryDetailsResponse)
async def get_memory_details(crew_name: str):
    """
    Get detailed information about a crew's memory.
    
    Args:
        crew_name: Name of the crew
        
    Returns:
        MemoryDetailsResponse: Detailed memory information
    """
    try:
        details = MemoryConfig.get_crew_memory_details(crew_name)
        if details:
            logger.info(f"Retrieved memory details for crew '{crew_name}'")
            return details
        else:
            logger.warning(f"No memory found for crew '{crew_name}'")
            raise HTTPException(
                status_code=404,
                detail=f"No memory found for crew '{crew_name}'"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting memory details for crew '{crew_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get memory details: {str(e)}"
        )


@memory_router.get("/stats", response_model=MemoryStatsResponse)
async def get_memory_stats(detailed: bool = False):
    """
    Get memory usage statistics.
    
    Args:
        detailed: Whether to include detailed stats per crew
        
    Returns:
        MemoryStatsResponse: Memory statistics
    """
    try:
        stats = MemoryConfig.get_memory_stats(detailed=detailed)
        logger.info(f"Retrieved memory stats for {stats['total_crews']} crews")
        return stats
    except Exception as e:
        logger.error(f"Error getting memory statistics: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get memory statistics: {str(e)}"
        )


@memory_router.get("/search", response_model=MemorySearchResponse)
async def search_memories(
    query: str = Query(..., description="Text to search for in memories"),
):
    """
    Search for memories containing specific text.
    
    Args:
        query: Text to search for in memories
        
    Returns:
        MemorySearchResponse: Search results
    """
    try:
        results = MemoryConfig.search_memories(query)
        search_results = [
            MemorySearchItem(crew_name=crew_name, snippet=snippet)
            for crew_name, snippet in results
        ]
        logger.info(f"Search for '{query}' found {len(search_results)} results")
        return MemorySearchResponse(
            results=search_results,
            count=len(search_results),
            query=query
        )
    except Exception as e:
        logger.error(f"Error searching memories: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search memories: {str(e)}"
        )


@memory_router.post("/cleanup", response_model=MemoryCleanupResponse)
async def cleanup_memories(
    background_tasks: BackgroundTasks,
    days: int = Query(30, description="Age in days for cleanup threshold", ge=1),
):
    """
    Remove memories older than a specified age.
    
    This operation will run in the background.
    
    Args:
        background_tasks: FastAPI background tasks
        days: Age in days (default: 30, minimum: 1)
        
    Returns:
        MemoryCleanupResponse: Result of cleanup operation
    """
    try:
        # Using a simple background task approach since the operation might take time
        def cleanup_task():
            try:
                count = MemoryConfig.cleanup_old_memories(days=days)
                logger.info(f"Cleaned up {count} memories older than {days} days")
            except Exception as e:
                logger.error(f"Background cleanup task failed: {e}")
        
        background_tasks.add_task(cleanup_task)
        
        logger.info(f"Memory cleanup started in background (threshold: {days} days)")
        return MemoryCleanupResponse(
            status="success",
            message=f"Memory cleanup started in background (threshold: {days} days)",
            count=0  # Count will be updated when the background task completes
        )
    except Exception as e:
        logger.error(f"Error initiating memory cleanup: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initiate memory cleanup: {str(e)}"
        ) 