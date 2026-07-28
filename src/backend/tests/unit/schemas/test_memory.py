"""
Unit tests for memory schemas.

Tests the functionality of Pydantic schemas for memory management operations
including validation, serialization, and field constraints.
"""
import pytest
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.memory import (
    MemoryListResponse, MemoryActionResponse, MemorySearchItem,
    MemorySearchResponse, MemoryLongTermInfo, MemoryComponentInfo,
    MemoryDetailsResponse, CrewMemoryStats, TimeStampedCrewInfo,
    MemoryStatsResponse, MemoryCleanupResponse
)


class TestMemoryListResponse:
    """Test cases for MemoryListResponse schema."""
    
    def test_valid_memory_list_response(self):
        """Test MemoryListResponse with all fields."""
        data = {
            "memories": ["crew1", "crew2", "crew3"],
            "count": 3
        }
        response = MemoryListResponse(**data)
        assert response.memories == ["crew1", "crew2", "crew3"]
        assert response.count == 3

    def test_empty_memory_list_response(self):
        """Test MemoryListResponse with empty list."""
        data = {
            "memories": [],
            "count": 0
        }
        response = MemoryListResponse(**data)
        assert response.memories == []
        assert response.count == 0

    def test_memory_list_response_missing_fields(self):
        """Test MemoryListResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemoryListResponse(memories=["crew1"])
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields


class TestMemoryActionResponse:
    """Test cases for MemoryActionResponse schema."""
    
    def test_valid_memory_action_response_success(self):
        """Test MemoryActionResponse for successful action."""
        data = {
            "status": "success",
            "message": "Memory reset successfully"
        }
        response = MemoryActionResponse(**data)
        assert response.status == "success"
        assert response.message == "Memory reset successfully"

    def test_valid_memory_action_response_failure(self):
        """Test MemoryActionResponse for failed action."""
        data = {
            "status": "failure",
            "message": "Failed to reset memory: access denied"
        }
        response = MemoryActionResponse(**data)
        assert response.status == "failure"
        assert response.message == "Failed to reset memory: access denied"

    def test_memory_action_response_missing_fields(self):
        """Test MemoryActionResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemoryActionResponse(status="success")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields


class TestMemorySearchItem:
    """Test cases for MemorySearchItem schema."""
    
    def test_valid_memory_search_item(self):
        """Test MemorySearchItem with all fields."""
        data = {
            "crew_name": "data_analysis_crew",
            "snippet": "This is a memory snippet containing the search query"
        }
        item = MemorySearchItem(**data)
        assert item.crew_name == "data_analysis_crew"
        assert item.snippet == "This is a memory snippet containing the search query"

    def test_memory_search_item_missing_fields(self):
        """Test MemorySearchItem validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemorySearchItem(crew_name="test_crew")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "snippet" in missing_fields


class TestMemorySearchResponse:
    """Test cases for MemorySearchResponse schema."""
    
    def test_valid_memory_search_response(self):
        """Test MemorySearchResponse with all fields."""
        search_items = [
            MemorySearchItem(crew_name="crew1", snippet="First snippet"),
            MemorySearchItem(crew_name="crew2", snippet="Second snippet")
        ]
        
        data = {
            "results": search_items,
            "count": 2,
            "query": "test search"
        }
        response = MemorySearchResponse(**data)
        assert len(response.results) == 2
        assert response.count == 2
        assert response.query == "test search"
        assert response.results[0].crew_name == "crew1"
        assert response.results[1].snippet == "Second snippet"

    def test_empty_memory_search_response(self):
        """Test MemorySearchResponse with no results."""
        data = {
            "results": [],
            "count": 0,
            "query": "no matches"
        }
        response = MemorySearchResponse(**data)
        assert len(response.results) == 0
        assert response.count == 0
        assert response.query == "no matches"

    def test_memory_search_response_missing_fields(self):
        """Test MemorySearchResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemorySearchResponse(results=[], count=0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "query" in missing_fields


class TestMemoryLongTermInfo:
    """Test cases for MemoryLongTermInfo schema."""
    
    def test_valid_memory_long_term_info_minimal(self):
        """Test MemoryLongTermInfo with minimal required fields."""
        data = {
            "path": "/path/to/memory.db",
            "size_bytes": 1024000
        }
        info = MemoryLongTermInfo(**data)
        assert info.path == "/path/to/memory.db"
        assert info.size_bytes == 1024000
        assert info.tables is None
        assert info.columns is None
        assert info.record_count is None
        assert info.sample_records is None
        assert info.error is None

    def test_valid_memory_long_term_info_full(self):
        """Test MemoryLongTermInfo with all fields."""
        data = {
            "path": "/path/to/full_memory.db",
            "size_bytes": 2048000,
            "tables": ["memories", "metadata"],
            "columns": ["id", "content", "timestamp"],
            "record_count": 150,
            "sample_records": [
                {"id": 1, "content": "Sample memory 1"},
                {"id": 2, "content": "Sample memory 2"}
            ],
            "error": None
        }
        info = MemoryLongTermInfo(**data)
        assert info.path == "/path/to/full_memory.db"
        assert info.size_bytes == 2048000
        assert info.tables == ["memories", "metadata"]
        assert info.columns == ["id", "content", "timestamp"]
        assert info.record_count == 150
        assert len(info.sample_records) == 2
        assert info.error is None

    def test_memory_long_term_info_with_error(self):
        """Test MemoryLongTermInfo with error."""
        data = {
            "path": "/path/to/corrupted.db",
            "size_bytes": 0,
            "error": "Database file is corrupted"
        }
        info = MemoryLongTermInfo(**data)
        assert info.path == "/path/to/corrupted.db"
        assert info.size_bytes == 0
        assert info.error == "Database file is corrupted"

    def test_memory_long_term_info_missing_fields(self):
        """Test MemoryLongTermInfo validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemoryLongTermInfo(path="/path/to/memory.db")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "size_bytes" in missing_fields


class TestMemoryComponentInfo:
    """Test cases for MemoryComponentInfo schema."""
    
    def test_valid_memory_component_info(self):
        """Test MemoryComponentInfo with all fields."""
        data = {
            "path": "/path/to/component",
            "size_bytes": 512000,
            "file_count": 25
        }
        info = MemoryComponentInfo(**data)
        assert info.path == "/path/to/component"
        assert info.size_bytes == 512000
        assert info.file_count == 25

    def test_memory_component_info_missing_fields(self):
        """Test MemoryComponentInfo validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemoryComponentInfo(path="/path/to/component", size_bytes=100)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "file_count" in missing_fields


class TestMemoryDetailsResponse:
    """Test cases for MemoryDetailsResponse schema."""
    
    def test_valid_memory_details_response_minimal(self):
        """Test MemoryDetailsResponse with minimal required fields."""
        data = {
            "crew_name": "test_crew",
            "memory_path": "/path/to/memory",
            "creation_date": "2023-01-01T12:00:00Z",
            "last_modified": "2023-01-02T12:00:00Z",
            "size_bytes": 1024000
        }
        response = MemoryDetailsResponse(**data)
        assert response.crew_name == "test_crew"
        assert response.memory_path == "/path/to/memory"
        assert response.creation_date == "2023-01-01T12:00:00Z"
        assert response.last_modified == "2023-01-02T12:00:00Z"
        assert response.size_bytes == 1024000
        assert response.long_term_memory is None
        assert response.short_term_memory is None
        assert response.entity_memory is None

    def test_valid_memory_details_response_full(self):
        """Test MemoryDetailsResponse with all components."""
        long_term = MemoryLongTermInfo(
            path="/path/to/longterm.db",
            size_bytes=2048000,
            record_count=100
        )
        short_term = MemoryComponentInfo(
            path="/path/to/shortterm",
            size_bytes=512000,
            file_count=10
        )
        entity = MemoryComponentInfo(
            path="/path/to/entity",
            size_bytes=256000,
            file_count=5
        )
        
        data = {
            "crew_name": "full_crew",
            "memory_path": "/path/to/full_memory",
            "creation_date": "2023-01-01T12:00:00Z",
            "last_modified": "2023-01-02T12:00:00Z",
            "size_bytes": 2816000,
            "long_term_memory": long_term,
            "short_term_memory": short_term,
            "entity_memory": entity
        }
        response = MemoryDetailsResponse(**data)
        assert response.crew_name == "full_crew"
        assert response.size_bytes == 2816000
        assert response.long_term_memory.record_count == 100
        assert response.short_term_memory.file_count == 10
        assert response.entity_memory.file_count == 5


class TestCrewMemoryStats:
    """Test cases for CrewMemoryStats schema."""
    
    def test_valid_crew_memory_stats(self):
        """Test CrewMemoryStats with all fields."""
        data = {
            "size": 1024.5,
            "last_modified": "2023-01-01T12:00:00Z",
            "messages_count": 150
        }
        stats = CrewMemoryStats(**data)
        assert stats.size == 1024.5
        assert stats.last_modified == "2023-01-01T12:00:00Z"
        assert stats.messages_count == 150

    def test_crew_memory_stats_without_messages(self):
        """Test CrewMemoryStats without optional messages_count."""
        data = {
            "size": 512.25,
            "last_modified": "2023-01-02T12:00:00Z"
        }
        stats = CrewMemoryStats(**data)
        assert stats.size == 512.25
        assert stats.last_modified == "2023-01-02T12:00:00Z"
        assert stats.messages_count is None

    def test_crew_memory_stats_missing_fields(self):
        """Test CrewMemoryStats validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            CrewMemoryStats(size=100.0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "last_modified" in missing_fields


class TestTimeStampedCrewInfo:
    """Test cases for TimeStampedCrewInfo schema."""
    
    def test_valid_timestamped_crew_info(self):
        """Test TimeStampedCrewInfo with all fields."""
        data = {
            "crew": "timestamped_crew",
            "timestamp": "2023-01-01T12:00:00Z"
        }
        info = TimeStampedCrewInfo(**data)
        assert info.crew == "timestamped_crew"
        assert info.timestamp == "2023-01-01T12:00:00Z"

    def test_timestamped_crew_info_missing_fields(self):
        """Test TimeStampedCrewInfo validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            TimeStampedCrewInfo(crew="test_crew")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "timestamp" in missing_fields


class TestMemoryStatsResponse:
    """Test cases for MemoryStatsResponse schema."""
    
    def test_valid_memory_stats_response(self):
        """Test MemoryStatsResponse with all fields."""
        oldest = TimeStampedCrewInfo(crew="old_crew", timestamp="2023-01-01T12:00:00Z")
        newest = TimeStampedCrewInfo(crew="new_crew", timestamp="2023-01-10T12:00:00Z")
        
        crew_details = {
            "crew1": CrewMemoryStats(size=100.0, last_modified="2023-01-05T12:00:00Z"),
            "crew2": CrewMemoryStats(size=200.0, last_modified="2023-01-06T12:00:00Z", messages_count=50)
        }
        
        data = {
            "total_crews": 2,
            "total_size": 300.0,
            "avg_size": 150.0,
            "oldest_memory": oldest,
            "newest_memory": newest,
            "crew_details": crew_details
        }
        response = MemoryStatsResponse(**data)
        assert response.total_crews == 2
        assert response.total_size == 300.0
        assert response.avg_size == 150.0
        assert response.oldest_memory.crew == "old_crew"
        assert response.newest_memory.crew == "new_crew"
        assert len(response.crew_details) == 2
        assert response.crew_details["crew1"].size == 100.0
        assert response.crew_details["crew2"].messages_count == 50

    def test_memory_stats_response_without_timestamps(self):
        """Test MemoryStatsResponse without timestamp fields."""
        data = {
            "total_crews": 1,
            "total_size": 100.0,
            "avg_size": 100.0,
            "crew_details": {
                "solo_crew": CrewMemoryStats(size=100.0, last_modified="2023-01-01T12:00:00Z")
            }
        }
        response = MemoryStatsResponse(**data)
        assert response.total_crews == 1
        assert response.oldest_memory is None
        assert response.newest_memory is None
        assert len(response.crew_details) == 1

    def test_memory_stats_response_missing_fields(self):
        """Test MemoryStatsResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemoryStatsResponse(total_crews=1, total_size=100.0)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "avg_size" in missing_fields
        assert "crew_details" in missing_fields


class TestMemoryCleanupResponse:
    """Test cases for MemoryCleanupResponse schema."""
    
    def test_valid_memory_cleanup_response_success(self):
        """Test MemoryCleanupResponse for successful cleanup."""
        data = {
            "status": "success",
            "message": "Cleaned up 5 old memories successfully",
            "count": 5
        }
        response = MemoryCleanupResponse(**data)
        assert response.status == "success"
        assert response.message == "Cleaned up 5 old memories successfully"
        assert response.count == 5

    def test_valid_memory_cleanup_response_failure(self):
        """Test MemoryCleanupResponse for failed cleanup."""
        data = {
            "status": "failure",
            "message": "Failed to clean up memories: permission denied",
            "count": 0
        }
        response = MemoryCleanupResponse(**data)
        assert response.status == "failure"
        assert response.message == "Failed to clean up memories: permission denied"
        assert response.count == 0

    def test_memory_cleanup_response_missing_fields(self):
        """Test MemoryCleanupResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            MemoryCleanupResponse(status="success", message="Cleanup completed")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields


class TestSchemaIntegration:
    """Integration tests for memory schema interactions."""
    
    def test_memory_management_workflow(self):
        """Test complete memory management workflow."""
        # List memories
        memory_list = MemoryListResponse(
            memories=["analytics_crew", "research_crew"],
            count=2
        )
        
        # Search memories
        search_items = [
            MemorySearchItem(crew_name="analytics_crew", snippet="data analysis results"),
            MemorySearchItem(crew_name="research_crew", snippet="research findings")
        ]
        search_response = MemorySearchResponse(
            results=search_items,
            count=2,
            query="analysis"
        )
        
        # Get detailed memory info
        long_term = MemoryLongTermInfo(
            path="/memories/analytics/longterm.db",
            size_bytes=1024000,
            record_count=100
        )
        short_term = MemoryComponentInfo(
            path="/memories/analytics/shortterm",
            size_bytes=256000,
            file_count=10
        )
        
        details = MemoryDetailsResponse(
            crew_name="analytics_crew",
            memory_path="/memories/analytics",
            creation_date="2023-01-01T12:00:00Z",
            last_modified="2023-01-05T12:00:00Z",
            size_bytes=1280000,
            long_term_memory=long_term,
            short_term_memory=short_term
        )
        
        # Reset memory
        reset_action = MemoryActionResponse(
            status="success",
            message="Memory reset completed successfully"
        )
        
        # Cleanup old memories
        cleanup_response = MemoryCleanupResponse(
            status="success",
            message="Cleaned up 3 old memory files",
            count=3
        )
        
        # Verify workflow
        assert memory_list.count == 2
        assert "analytics_crew" in memory_list.memories
        assert search_response.count == 2
        assert search_response.results[0].crew_name == "analytics_crew"
        assert details.crew_name == "analytics_crew"
        assert details.long_term_memory.record_count == 100
        assert reset_action.status == "success"
        assert cleanup_response.count == 3

    def test_memory_statistics_workflow(self):
        """Test memory statistics workflow."""
        # Create individual crew stats
        crew1_stats = CrewMemoryStats(
            size=500.0,
            last_modified="2023-01-01T12:00:00Z",
            messages_count=75
        )
        crew2_stats = CrewMemoryStats(
            size=750.0,
            last_modified="2023-01-05T12:00:00Z",
            messages_count=125
        )
        
        # Create timestamp info
        oldest = TimeStampedCrewInfo(crew="crew1", timestamp="2023-01-01T12:00:00Z")
        newest = TimeStampedCrewInfo(crew="crew2", timestamp="2023-01-05T12:00:00Z")
        
        # Create overall stats
        stats_response = MemoryStatsResponse(
            total_crews=2,
            total_size=1250.0,
            avg_size=625.0,
            oldest_memory=oldest,
            newest_memory=newest,
            crew_details={
                "crew1": crew1_stats,
                "crew2": crew2_stats
            }
        )
        
        # Verify statistics
        assert stats_response.total_crews == 2
        assert stats_response.total_size == 1250.0
        assert stats_response.avg_size == 625.0
        assert stats_response.oldest_memory.crew == "crew1"
        assert stats_response.newest_memory.crew == "crew2"
        assert stats_response.crew_details["crew1"].messages_count == 75
        assert stats_response.crew_details["crew2"].messages_count == 125

    def test_memory_search_and_details_workflow(self):
        """Test memory search and details workflow."""
        # Search for specific content
        search_item = MemorySearchItem(
            crew_name="data_team",
            snippet="customer behavior analysis shows..."
        )
        search_response = MemorySearchResponse(
            results=[search_item],
            count=1,
            query="customer behavior"
        )
        
        # Get details for the found crew
        long_term_info = MemoryLongTermInfo(
            path="/memories/data_team/longterm.db",
            size_bytes=2048000,
            tables=["conversations", "entities", "metadata"],
            columns=["id", "content", "timestamp", "entity_type"],
            record_count=500,
            sample_records=[
                {"id": 1, "content": "customer behavior analysis..."},
                {"id": 2, "content": "market trends indicate..."}
            ]
        )
        
        entity_info = MemoryComponentInfo(
            path="/memories/data_team/entities",
            size_bytes=128000,
            file_count=3
        )
        
        details_response = MemoryDetailsResponse(
            crew_name="data_team",
            memory_path="/memories/data_team",
            creation_date="2023-01-01T09:00:00Z",
            last_modified="2023-01-10T15:30:00Z",
            size_bytes=2176000,
            long_term_memory=long_term_info,
            entity_memory=entity_info
        )
        
        # Verify search and details workflow
        assert search_response.count == 1
        assert search_response.results[0].crew_name == "data_team"
        assert "customer behavior" in search_response.results[0].snippet
        assert details_response.crew_name == "data_team"
        assert details_response.long_term_memory.record_count == 500
        assert details_response.entity_memory.file_count == 3
        assert len(details_response.long_term_memory.sample_records) == 2