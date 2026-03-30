"""
Pydantic schemas for memory management operations.

These schemas define the data structures for memory-related API operations,
such as listing, searching, resetting, and retrieving information about
CrewAI agent memories.
"""

from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field


class MemoryListResponse(BaseModel):
    """Response model for listing crew memories."""
    
    memories: List[str] = Field(
        description="List of crew names with memory storage"
    )
    count: int = Field(
        description="Total count of crew memories"
    )


class MemoryActionResponse(BaseModel):
    """Response model for memory actions like reset or delete."""
    
    status: str = Field(
        description="Status of the action (success/failure)"
    )
    message: str = Field(
        description="Descriptive message about the action result"
    )


class MemorySearchItem(BaseModel):
    """Individual memory search result item."""
    
    crew_name: str = Field(
        description="Name of the crew where the memory was found"
    )
    snippet: str = Field(
        description="Text snippet containing the search query"
    )


class MemorySearchResponse(BaseModel):
    """Response model for memory search operations."""
    
    results: List[MemorySearchItem] = Field(
        description="List of memory search results"
    )
    count: int = Field(
        description="Total number of results found"
    )
    query: str = Field(
        description="The original search query"
    )


class MemoryLongTermInfo(BaseModel):
    """Information about long-term memory database."""
    
    path: str = Field(
        description="Path to the long-term memory database"
    )
    size_bytes: int = Field(
        description="Size of the database in bytes"
    )
    tables: Optional[List[str]] = Field(
        None,
        description="List of tables in the database"
    )
    columns: Optional[List[str]] = Field(
        None,
        description="List of columns in the main memory table"
    )
    record_count: Optional[int] = Field(
        None,
        description="Number of memory records"
    )
    sample_records: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Sample memory records"
    )
    error: Optional[str] = Field(
        None,
        description="Error message if database reading failed"
    )


class MemoryComponentInfo(BaseModel):
    """Information about memory components (short-term and entity)."""
    
    path: str = Field(
        description="Path to the memory component"
    )
    size_bytes: int = Field(
        description="Size of the component in bytes"
    )
    file_count: int = Field(
        description="Number of files in the component"
    )


class MemoryDetailsResponse(BaseModel):
    """Response model for detailed memory information."""
    
    crew_name: str = Field(
        description="Name of the crew"
    )
    memory_path: str = Field(
        description="Path to the memory directory"
    )
    creation_date: str = Field(
        description="Creation date of the memory (ISO format)"
    )
    last_modified: str = Field(
        description="Last modification date of the memory (ISO format)"
    )
    size_bytes: int = Field(
        description="Total size of the memory in bytes"
    )
    long_term_memory: Optional[MemoryLongTermInfo] = Field(
        None,
        description="Information about long-term memory database"
    )
    short_term_memory: Optional[MemoryComponentInfo] = Field(
        None,
        description="Information about short-term memory"
    )
    entity_memory: Optional[MemoryComponentInfo] = Field(
        None,
        description="Information about entity memory"
    )


class CrewMemoryStats(BaseModel):
    """Statistics about a crew's memory."""
    
    size: float = Field(
        description="Size of the memory in KB"
    )
    last_modified: str = Field(
        description="Last modification date (ISO format)"
    )
    messages_count: Optional[int] = Field(
        None,
        description="Number of messages in memory (if available)"
    )


class TimeStampedCrewInfo(BaseModel):
    """Timestamped information about a crew."""
    
    crew: str = Field(
        description="Name of the crew"
    )
    timestamp: str = Field(
        description="Timestamp (ISO format)"
    )


class MemoryStatsResponse(BaseModel):
    """Response model for memory statistics."""
    
    total_crews: int = Field(
        description="Total number of crews with memory"
    )
    total_size: float = Field(
        description="Total size of all memories in KB"
    )
    avg_size: float = Field(
        description="Average size of memory per crew in KB"
    )
    oldest_memory: Optional[TimeStampedCrewInfo] = Field(
        None,
        description="Information about the oldest memory"
    )
    newest_memory: Optional[TimeStampedCrewInfo] = Field(
        None,
        description="Information about the newest memory"
    )
    crew_details: Dict[str, CrewMemoryStats] = Field(
        description="Detailed stats for each crew"
    )


class MemoryCleanupResponse(BaseModel):
    """Response model for memory cleanup operations."""
    
    status: str = Field(
        description="Status of the cleanup (success/failure)"
    )
    message: str = Field(
        description="Descriptive message about the cleanup result"
    )
    count: int = Field(
        description="Number of memories cleaned up"
    ) 