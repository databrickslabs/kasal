"""
Unit tests for execution logs schemas.

Tests the functionality of Pydantic schemas for execution log operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.execution_logs import (
    LogMessage, ExecutionLogResponse, ExecutionLogsResponse
)


class TestLogMessage:
    """Test cases for LogMessage schema."""
    
    def test_valid_log_message_live(self):
        """Test LogMessage with live type."""
        log_data = {
            "execution_id": "exec_123",
            "content": "Starting data analysis",
            "timestamp": "2023-01-01T12:00:00Z",
            "type": "live"
        }
        log_message = LogMessage(**log_data)
        assert log_message.execution_id == "exec_123"
        assert log_message.content == "Starting data analysis"
        assert log_message.timestamp == "2023-01-01T12:00:00Z"
        assert log_message.type == "live"
    
    def test_valid_log_message_historical(self):
        """Test LogMessage with historical type."""
        log_data = {
            "execution_id": "exec_456",
            "content": "Analysis completed successfully",
            "timestamp": "2023-01-01T12:30:00Z",
            "type": "historical"
        }
        log_message = LogMessage(**log_data)
        assert log_message.execution_id == "exec_456"
        assert log_message.content == "Analysis completed successfully"
        assert log_message.timestamp == "2023-01-01T12:30:00Z"
        assert log_message.type == "historical"
    
    def test_log_message_missing_required_fields(self):
        """Test LogMessage validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            LogMessage(execution_id="exec_123")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "content" in missing_fields
        assert "timestamp" in missing_fields
        assert "type" in missing_fields
    
    def test_log_message_invalid_type(self):
        """Test LogMessage with invalid type value."""
        log_data = {
            "execution_id": "exec_123",
            "content": "Test message",
            "timestamp": "2023-01-01T12:00:00Z",
            "type": "invalid_type"
        }
        with pytest.raises(ValidationError) as exc_info:
            LogMessage(**log_data)
        
        errors = exc_info.value.errors()
        assert any(error["type"] == "literal_error" for error in errors)
    
    def test_log_message_empty_content(self):
        """Test LogMessage with empty content."""
        log_data = {
            "execution_id": "exec_123",
            "content": "",
            "timestamp": "2023-01-01T12:00:00Z",
            "type": "live"
        }
        log_message = LogMessage(**log_data)
        assert log_message.content == ""
    
    def test_log_message_long_content(self):
        """Test LogMessage with long content."""
        long_content = "A" * 10000
        log_data = {
            "execution_id": "exec_123",
            "content": long_content,
            "timestamp": "2023-01-01T12:00:00Z",
            "type": "live"
        }
        log_message = LogMessage(**log_data)
        assert log_message.content == long_content
    
    def test_log_message_special_characters(self):
        """Test LogMessage with special characters in content."""
        special_content = "Processing data with symbols: !@#$%^&*(){}[]|\\:;\"'<>,.?/~`"
        log_data = {
            "execution_id": "exec_123",
            "content": special_content,
            "timestamp": "2023-01-01T12:00:00Z",
            "type": "historical"
        }
        log_message = LogMessage(**log_data)
        assert log_message.content == special_content
    
    def test_log_message_multiline_content(self):
        """Test LogMessage with multiline content."""
        multiline_content = """Line 1: Starting process
Line 2: Loading data
Line 3: Processing complete"""
        log_data = {
            "execution_id": "exec_123",
            "content": multiline_content,
            "timestamp": "2023-01-01T12:00:00Z",
            "type": "live"
        }
        log_message = LogMessage(**log_data)
        assert log_message.content == multiline_content


class TestExecutionLogResponse:
    """Test cases for ExecutionLogResponse schema."""
    
    def test_valid_execution_log_response(self):
        """Test ExecutionLogResponse with valid data."""
        response_data = {
            "content": "Task completed successfully",
            "timestamp": "2023-01-01T14:30:00Z"
        }
        log_response = ExecutionLogResponse(**response_data)
        assert log_response.content == "Task completed successfully"
        assert log_response.timestamp == "2023-01-01T14:30:00Z"
    
    def test_execution_log_response_missing_fields(self):
        """Test ExecutionLogResponse validation with missing fields."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionLogResponse(content="Test content")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "timestamp" in missing_fields
        
        with pytest.raises(ValidationError) as exc_info:
            ExecutionLogResponse(timestamp="2023-01-01T12:00:00Z")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "content" in missing_fields
    
    def test_execution_log_response_empty_content(self):
        """Test ExecutionLogResponse with empty content."""
        response_data = {
            "content": "",
            "timestamp": "2023-01-01T14:30:00Z"
        }
        log_response = ExecutionLogResponse(**response_data)
        assert log_response.content == ""
    
    def test_execution_log_response_various_timestamps(self):
        """Test ExecutionLogResponse with various timestamp formats."""
        timestamps = [
            "2023-01-01T12:00:00Z",
            "2023-12-31T23:59:59Z",
            "2023-06-15T09:30:45.123Z",
            "2023-01-01T00:00:00.000000Z"
        ]
        
        for timestamp in timestamps:
            response_data = {
                "content": f"Log for {timestamp}",
                "timestamp": timestamp
            }
            log_response = ExecutionLogResponse(**response_data)
            assert log_response.timestamp == timestamp
    
    def test_execution_log_response_json_content(self):
        """Test ExecutionLogResponse with JSON-like content."""
        json_content = '{"status": "success", "processed_items": 150, "errors": []}'
        response_data = {
            "content": json_content,
            "timestamp": "2023-01-01T14:30:00Z"
        }
        log_response = ExecutionLogResponse(**response_data)
        assert log_response.content == json_content


class TestExecutionLogsResponse:
    """Test cases for ExecutionLogsResponse schema."""
    
    def test_valid_execution_logs_response_empty(self):
        """Test ExecutionLogsResponse with empty logs list."""
        response_data = {"logs": []}
        logs_response = ExecutionLogsResponse(**response_data)
        assert logs_response.logs == []
        assert len(logs_response.logs) == 0
    
    def test_valid_execution_logs_response_single_log(self):
        """Test ExecutionLogsResponse with single log."""
        log_data = {
            "content": "Single log entry",
            "timestamp": "2023-01-01T12:00:00Z"
        }
        response_data = {"logs": [log_data]}
        logs_response = ExecutionLogsResponse(**response_data)
        assert len(logs_response.logs) == 1
        assert logs_response.logs[0].content == "Single log entry"
        assert logs_response.logs[0].timestamp == "2023-01-01T12:00:00Z"
    
    def test_valid_execution_logs_response_multiple_logs(self):
        """Test ExecutionLogsResponse with multiple logs."""
        logs_data = [
            {
                "content": "Starting execution",
                "timestamp": "2023-01-01T12:00:00Z"
            },
            {
                "content": "Processing data",
                "timestamp": "2023-01-01T12:05:00Z"
            },
            {
                "content": "Execution completed",
                "timestamp": "2023-01-01T12:10:00Z"
            }
        ]
        response_data = {"logs": logs_data}
        logs_response = ExecutionLogsResponse(**response_data)
        assert len(logs_response.logs) == 3
        assert logs_response.logs[0].content == "Starting execution"
        assert logs_response.logs[1].content == "Processing data"
        assert logs_response.logs[2].content == "Execution completed"
    
    def test_execution_logs_response_missing_logs_field(self):
        """Test ExecutionLogsResponse validation with missing logs field."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionLogsResponse()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "logs" in missing_fields
    
    def test_execution_logs_response_invalid_log_structure(self):
        """Test ExecutionLogsResponse with invalid log structure."""
        # Missing content in one log
        invalid_logs_data = [
            {
                "content": "Valid log",
                "timestamp": "2023-01-01T12:00:00Z"
            },
            {
                "timestamp": "2023-01-01T12:05:00Z"  # Missing content
            }
        ]
        response_data = {"logs": invalid_logs_data}
        with pytest.raises(ValidationError) as exc_info:
            ExecutionLogsResponse(**response_data)
        
        errors = exc_info.value.errors()
        assert any("content" in str(error) for error in errors)
    
    def test_execution_logs_response_large_list(self):
        """Test ExecutionLogsResponse with large list of logs."""
        logs_data = []
        for i in range(1000):
            logs_data.append({
                "content": f"Log entry {i}",
                "timestamp": f"2023-01-01T{i % 24:02d}:{i % 60:02d}:00Z"
            })
        
        response_data = {"logs": logs_data}
        logs_response = ExecutionLogsResponse(**response_data)
        assert len(logs_response.logs) == 1000
        assert logs_response.logs[0].content == "Log entry 0"
        assert logs_response.logs[999].content == "Log entry 999"


class TestSchemaIntegration:
    """Integration tests for execution logs schema interactions."""
    
    def test_log_message_to_execution_log_response_conversion(self):
        """Test conceptual conversion from LogMessage to ExecutionLogResponse."""
        # Create a LogMessage
        log_message = LogMessage(
            execution_id="exec_123",
            content="Processing workflow step 1",
            timestamp="2023-01-01T12:00:00Z",
            type="live"
        )
        
        # Convert to ExecutionLogResponse format (conceptually)
        log_response = ExecutionLogResponse(
            content=log_message.content,
            timestamp=log_message.timestamp
        )
        
        assert log_response.content == log_message.content
        assert log_response.timestamp == log_message.timestamp
    
    def test_execution_logs_workflow(self):
        """Test complete execution logs workflow."""
        # Create multiple LogMessages
        log_messages = [
            LogMessage(
                execution_id="exec_workflow_001",
                content="Workflow started",
                timestamp="2023-01-01T10:00:00Z",
                type="historical"
            ),
            LogMessage(
                execution_id="exec_workflow_001",
                content="Step 1: Data loading in progress",
                timestamp="2023-01-01T10:05:00Z",
                type="live"
            ),
            LogMessage(
                execution_id="exec_workflow_001",
                content="Step 2: Data processing completed",
                timestamp="2023-01-01T10:15:00Z",
                type="live"
            ),
            LogMessage(
                execution_id="exec_workflow_001",
                content="Workflow completed successfully",
                timestamp="2023-01-01T10:20:00Z",
                type="historical"
            )
        ]
        
        # Convert to ExecutionLogResponse format
        log_responses = []
        for log_msg in log_messages:
            log_responses.append(ExecutionLogResponse(
                content=log_msg.content,
                timestamp=log_msg.timestamp
            ))
        
        # Create ExecutionLogsResponse
        logs_response = ExecutionLogsResponse(logs=log_responses)
        
        # Verify workflow
        assert len(logs_response.logs) == 4
        assert logs_response.logs[0].content == "Workflow started"
        assert logs_response.logs[-1].content == "Workflow completed successfully"
        assert all(log.timestamp.startswith("2023-01-01T10:") for log in logs_response.logs)
    
    def test_error_handling_scenarios(self):
        """Test error handling in execution logs."""
        # Error log message
        error_log = LogMessage(
            execution_id="exec_error_001",
            content="ERROR: Failed to connect to database - Connection timeout after 30 seconds",
            timestamp="2023-01-01T12:00:00Z",
            type="live"
        )
        
        # Warning log message
        warning_log = LogMessage(
            execution_id="exec_error_001",
            content="WARNING: Retrying operation (attempt 2 of 3)",
            timestamp="2023-01-01T12:01:00Z",
            type="live"
        )
        
        # Success log message
        success_log = LogMessage(
            execution_id="exec_error_001",
            content="INFO: Connection established successfully on retry",
            timestamp="2023-01-01T12:02:00Z",
            type="live"
        )
        
        # Create response with error logs
        error_response = ExecutionLogsResponse(logs=[
            ExecutionLogResponse(content=error_log.content, timestamp=error_log.timestamp),
            ExecutionLogResponse(content=warning_log.content, timestamp=warning_log.timestamp),
            ExecutionLogResponse(content=success_log.content, timestamp=success_log.timestamp)
        ])
        
        assert len(error_response.logs) == 3
        assert "ERROR:" in error_response.logs[0].content
        assert "WARNING:" in error_response.logs[1].content
        assert "INFO:" in error_response.logs[2].content
    
    def test_timestamp_ordering(self):
        """Test logs with timestamp ordering scenarios."""
        # Logs with timestamps in different orders
        unordered_logs = [
            ExecutionLogResponse(
                content="Log 3",
                timestamp="2023-01-01T12:30:00Z"
            ),
            ExecutionLogResponse(
                content="Log 1", 
                timestamp="2023-01-01T12:10:00Z"
            ),
            ExecutionLogResponse(
                content="Log 2",
                timestamp="2023-01-01T12:20:00Z"
            )
        ]
        
        logs_response = ExecutionLogsResponse(logs=unordered_logs)
        
        # Verify logs are stored as provided (no automatic sorting)
        assert logs_response.logs[0].content == "Log 3"
        assert logs_response.logs[1].content == "Log 1"
        assert logs_response.logs[2].content == "Log 2"
        
        # Verify timestamps are preserved
        assert logs_response.logs[0].timestamp == "2023-01-01T12:30:00Z"
        assert logs_response.logs[1].timestamp == "2023-01-01T12:10:00Z"
        assert logs_response.logs[2].timestamp == "2023-01-01T12:20:00Z"