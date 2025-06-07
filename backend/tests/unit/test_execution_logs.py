"""
Unit tests for execution logs models.

Tests the functionality of ExecutionLog model including
field validation, indexing, and multi-group support.
"""
import pytest
from datetime import datetime
from unittest.mock import patch

from src.models.execution_logs import ExecutionLog


class TestExecutionLog:
    """Test cases for ExecutionLog model."""
    
    def test_execution_log_creation(self):
        """Test basic ExecutionLog creation."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log message"
        )
        
        assert log.execution_id == "exec_123"
        assert log.content == "Test log message"
    
    def test_execution_log_required_fields(self):
        """Test ExecutionLog with required fields."""
        log = ExecutionLog(
            execution_id="exec_456",
            content="Another log message"
        )
        
        assert log.execution_id == "exec_456"
        assert log.content == "Another log message"
        assert log.group_id is None
        assert log.tenant_id is None
        assert log.group_email is None
        assert log.tenant_email is None
    
    def test_execution_log_timestamp_default(self):
        """Test that timestamp is set by default."""
        with patch('src.models.execution_logs.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            log = ExecutionLog(
                execution_id="exec_123",
                content="Test log message"
            )
            
            assert log.timestamp == mock_now
    
    def test_execution_log_custom_timestamp(self):
        """Test ExecutionLog with custom timestamp."""
        custom_time = datetime(2023, 1, 1, 13, 0, 0)
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log message",
            timestamp=custom_time
        )
        
        assert log.timestamp == custom_time
    
    def test_execution_log_group_fields(self):
        """Test ExecutionLog with group-related fields."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Group log message",
            group_id="group_456",
            group_email="user@group.com"
        )
        
        assert log.group_id == "group_456"
        assert log.group_email == "user@group.com"
        assert log.tenant_id is None  # Legacy field should be None
        assert log.tenant_email is None  # Legacy field should be None
    
    def test_execution_log_legacy_tenant_fields(self):
        """Test ExecutionLog with legacy tenant fields."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Tenant log message",
            tenant_id="tenant_789",
            tenant_email="user@tenant.com"
        )
        
        assert log.tenant_id == "tenant_789"
        assert log.tenant_email == "user@tenant.com"
        assert log.group_id is None  # New field should be None
        assert log.group_email is None  # New field should be None
    
    def test_execution_log_all_fields(self):
        """Test ExecutionLog with all fields populated."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Complete log message",
            timestamp=datetime(2023, 1, 1, 14, 0, 0),
            group_id="group_456",
            group_email="user@group.com",
            tenant_id="tenant_789",  # Legacy compatibility
            tenant_email="user@tenant.com"  # Legacy compatibility
        )
        
        assert log.execution_id == "exec_123"
        assert log.content == "Complete log message"
        assert log.timestamp == datetime(2023, 1, 1, 14, 0, 0)
        assert log.group_id == "group_456"
        assert log.group_email == "user@group.com"
        assert log.tenant_id == "tenant_789"
        assert log.tenant_email == "user@tenant.com"
    
    def test_execution_log_tablename(self):
        """Test ExecutionLog table name."""
        assert ExecutionLog.__tablename__ == "execution_logs"
    
    def test_execution_log_long_content(self):
        """Test ExecutionLog with long content."""
        long_content = "This is a very long log message. " * 100
        log = ExecutionLog(
            execution_id="exec_123",
            content=long_content
        )
        
        assert log.content == long_content
        assert len(log.content) > 1000
    
    def test_execution_log_multiline_content(self):
        """Test ExecutionLog with multiline content."""
        multiline_content = """This is line 1
This is line 2
This is line 3 with special characters: !@#$%^&*()
This is line 4 with unicode: éñ中文"""
        
        log = ExecutionLog(
            execution_id="exec_123",
            content=multiline_content
        )
        
        assert log.content == multiline_content
        assert "\n" in log.content
    
    def test_execution_log_empty_content(self):
        """Test ExecutionLog with empty content."""
        # Note: content is nullable=False, so empty string should be allowed
        log = ExecutionLog(
            execution_id="exec_123",
            content=""
        )
        
        assert log.content == ""
    
    def test_execution_log_special_characters_in_execution_id(self):
        """Test ExecutionLog with special characters in execution_id."""
        special_execution_id = "exec-123_456.789"
        log = ExecutionLog(
            execution_id=special_execution_id,
            content="Log for special execution ID"
        )
        
        assert log.execution_id == special_execution_id


class TestExecutionLogIndexes:
    """Test cases for ExecutionLog table indexes."""
    
    def test_table_args_defined(self):
        """Test that table args with indexes are defined."""
        assert hasattr(ExecutionLog, '__table_args__')
        assert ExecutionLog.__table_args__ is not None
    
    def test_execution_logs_indexes_exist(self):
        """Test that expected indexes are defined."""
        table_args = ExecutionLog.__table_args__
        
        # Convert to list of index names for easier testing
        index_names = []
        for arg in table_args:
            if hasattr(arg, 'name'):
                index_names.append(arg.name)
        
        # Check that expected indexes exist
        expected_indexes = [
            'idx_execution_logs_exec_id_timestamp',
            'idx_execution_logs_group_timestamp',
            'idx_execution_logs_group_exec_id',
            'idx_execution_logs_tenant_timestamp',
            'idx_execution_logs_tenant_exec_id'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names
    
    def test_execution_id_timestamp_index(self):
        """Test execution_id and timestamp composite index."""
        table_args = ExecutionLog.__table_args__
        
        # Find the specific index
        exec_timestamp_index = None
        for arg in table_args:
            if hasattr(arg, 'name') and arg.name == 'idx_execution_logs_exec_id_timestamp':
                exec_timestamp_index = arg
                break
        
        assert exec_timestamp_index is not None
        # Check that the index includes the expected columns
        column_names = [col.name for col in exec_timestamp_index.columns]
        assert 'execution_id' in column_names
        assert 'timestamp' in column_names
    
    def test_group_based_indexes(self):
        """Test group-based indexes."""
        table_args = ExecutionLog.__table_args__
        
        # Find group-related indexes
        group_indexes = []
        for arg in table_args:
            if hasattr(arg, 'name') and 'group' in arg.name:
                group_indexes.append(arg)
        
        assert len(group_indexes) >= 2  # group_timestamp and group_exec_id
        
        # Check specific indexes
        index_names = [idx.name for idx in group_indexes]
        assert 'idx_execution_logs_group_timestamp' in index_names
        assert 'idx_execution_logs_group_exec_id' in index_names
    
    def test_tenant_based_indexes_legacy(self):
        """Test legacy tenant-based indexes."""
        table_args = ExecutionLog.__table_args__
        
        # Find tenant-related indexes (legacy)
        tenant_indexes = []
        for arg in table_args:
            if hasattr(arg, 'name') and 'tenant' in arg.name:
                tenant_indexes.append(arg)
        
        assert len(tenant_indexes) >= 2  # tenant_timestamp and tenant_exec_id
        
        # Check specific indexes
        index_names = [idx.name for idx in tenant_indexes]
        assert 'idx_execution_logs_tenant_timestamp' in index_names
        assert 'idx_execution_logs_tenant_exec_id' in index_names


class TestExecutionLogFieldTypes:
    """Test cases for ExecutionLog field types and constraints."""
    
    def test_execution_log_field_existence(self):
        """Test that all expected fields exist."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log"
        )
        
        # Check field existence
        assert hasattr(log, 'id')
        assert hasattr(log, 'execution_id')
        assert hasattr(log, 'content')
        assert hasattr(log, 'timestamp')
        assert hasattr(log, 'group_id')
        assert hasattr(log, 'tenant_id')
        assert hasattr(log, 'group_email')
        assert hasattr(log, 'tenant_email')
    
    def test_execution_log_id_field(self):
        """Test ExecutionLog id field properties."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log"
        )
        
        # id should be None before persisting (auto-generated)
        assert log.id is None
    
    def test_execution_log_string_fields_length(self):
        """Test string field length constraints."""
        # Test group_id field (should accept 100 characters)
        group_id_100 = "g" * 100
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log",
            group_id=group_id_100
        )
        assert log.group_id == group_id_100
        
        # Test group_email field (should accept 255 characters)
        group_email_255 = "u" * 240 + "@example.com"  # 255 chars total
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log",
            group_email=group_email_255
        )
        assert log.group_email == group_email_255
    
    def test_execution_log_nullable_fields(self):
        """Test nullable field behavior."""
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log"
        )
        
        # These fields should be nullable
        assert log.group_id is None
        assert log.tenant_id is None
        assert log.group_email is None
        assert log.tenant_email is None
    
    def test_execution_log_non_nullable_fields(self):
        """Test non-nullable field requirements."""
        # execution_id and content are non-nullable
        log = ExecutionLog(
            execution_id="exec_123",
            content="Test log"
        )
        
        assert log.execution_id is not None
        assert log.content is not None


class TestExecutionLogUsagePatterns:
    """Test cases for common ExecutionLog usage patterns."""
    
    def test_execution_log_chronological_logging(self):
        """Test chronological logging pattern."""
        execution_id = "exec_123"
        
        # Log sequence
        logs = [
            ExecutionLog(
                execution_id=execution_id,
                content="Starting execution",
                timestamp=datetime(2023, 1, 1, 12, 0, 0)
            ),
            ExecutionLog(
                execution_id=execution_id,
                content="Processing step 1",
                timestamp=datetime(2023, 1, 1, 12, 1, 0)
            ),
            ExecutionLog(
                execution_id=execution_id,
                content="Processing step 2",
                timestamp=datetime(2023, 1, 1, 12, 2, 0)
            ),
            ExecutionLog(
                execution_id=execution_id,
                content="Execution completed",
                timestamp=datetime(2023, 1, 1, 12, 3, 0)
            )
        ]
        
        # Verify all logs belong to same execution
        for log in logs:
            assert log.execution_id == execution_id
        
        # Verify chronological order
        for i in range(1, len(logs)):
            assert logs[i].timestamp > logs[i-1].timestamp
    
    def test_execution_log_group_isolation(self):
        """Test group isolation pattern."""
        # Group A logs
        group_a_logs = [
            ExecutionLog(
                execution_id="exec_a1",
                content="Group A execution 1",
                group_id="group_a",
                group_email="user@groupa.com"
            ),
            ExecutionLog(
                execution_id="exec_a2",
                content="Group A execution 2",
                group_id="group_a",
                group_email="user@groupa.com"
            )
        ]
        
        # Group B logs
        group_b_logs = [
            ExecutionLog(
                execution_id="exec_b1",
                content="Group B execution 1",
                group_id="group_b",
                group_email="user@groupb.com"
            )
        ]
        
        # Verify group isolation
        for log in group_a_logs:
            assert log.group_id == "group_a"
            assert log.group_email == "user@groupa.com"
        
        for log in group_b_logs:
            assert log.group_id == "group_b"
            assert log.group_email == "user@groupb.com"
    
    def test_execution_log_error_logging(self):
        """Test error logging pattern."""
        execution_id = "exec_123"
        
        # Normal log
        info_log = ExecutionLog(
            execution_id=execution_id,
            content="Processing started successfully"
        )
        
        # Error log
        error_log = ExecutionLog(
            execution_id=execution_id,
            content="ERROR: Failed to process item due to validation error"
        )
        
        # Warning log
        warning_log = ExecutionLog(
            execution_id=execution_id,
            content="WARNING: Retrying failed operation"
        )
        
        # Verify all logs are for same execution
        assert info_log.execution_id == execution_id
        assert error_log.execution_id == execution_id
        assert warning_log.execution_id == execution_id
        
        # Verify content contains appropriate markers
        assert "ERROR:" in error_log.content
        assert "WARNING:" in warning_log.content
    
    def test_execution_log_migration_compatibility(self):
        """Test migration compatibility between tenant and group fields."""
        # Legacy tenant-based log
        tenant_log = ExecutionLog(
            execution_id="exec_legacy",
            content="Legacy tenant log",
            tenant_id="tenant_123",
            tenant_email="user@tenant.com"
        )
        
        # New group-based log
        group_log = ExecutionLog(
            execution_id="exec_new",
            content="New group log",
            group_id="group_456",
            group_email="user@group.com"
        )
        
        # Verify both can coexist
        assert tenant_log.tenant_id == "tenant_123"
        assert tenant_log.group_id is None
        
        assert group_log.group_id == "group_456"
        assert group_log.tenant_id is None
    
    def test_execution_log_batch_logging(self):
        """Test batch logging pattern."""
        execution_id = "exec_batch_123"
        batch_size = 5
        
        # Create batch of logs
        batch_logs = []
        for i in range(batch_size):
            log = ExecutionLog(
                execution_id=execution_id,
                content=f"Batch item {i+1} processed",
                timestamp=datetime(2023, 1, 1, 12, i, 0)
            )
            batch_logs.append(log)
        
        # Verify batch properties
        assert len(batch_logs) == batch_size
        
        for i, log in enumerate(batch_logs):
            assert log.execution_id == execution_id
            assert f"item {i+1}" in log.content
    
    def test_execution_log_structured_content(self):
        """Test structured content logging."""
        # JSON-like structured content
        structured_content = """{"level": "INFO", "message": "User action completed", "details": {"user_id": "123", "action": "create_workflow", "duration_ms": 1500}}"""
        
        log = ExecutionLog(
            execution_id="exec_123",
            content=structured_content
        )
        
        assert log.content == structured_content
        assert "INFO" in log.content
        assert "user_id" in log.content
        assert "duration_ms" in log.content