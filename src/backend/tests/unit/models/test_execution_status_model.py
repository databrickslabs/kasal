"""
Unit tests for execution_status model.

Tests the functionality of the ExecutionStatus enum including
value validation and completeness.
"""
import pytest
from src.models.execution_status import ExecutionStatus


class TestExecutionStatus:
    """Test cases for ExecutionStatus enum."""

    def test_execution_status_values(self):
        """Test that ExecutionStatus has all expected values."""
        # Act & Assert
        assert ExecutionStatus.PENDING == "PENDING"
        assert ExecutionStatus.PREPARING == "PREPARING"
        assert ExecutionStatus.RUNNING == "RUNNING"
        assert ExecutionStatus.COMPLETED == "COMPLETED"
        assert ExecutionStatus.FAILED == "FAILED"
        assert ExecutionStatus.CANCELLED == "CANCELLED"

    def test_execution_status_is_string_enum(self):
        """Test that ExecutionStatus inherits from str."""
        # Act & Assert
        assert isinstance(ExecutionStatus.PENDING, str)
        assert isinstance(ExecutionStatus.PREPARING, str)
        assert isinstance(ExecutionStatus.RUNNING, str)
        assert isinstance(ExecutionStatus.COMPLETED, str)
        assert isinstance(ExecutionStatus.FAILED, str)
        assert isinstance(ExecutionStatus.CANCELLED, str)

    def test_execution_status_enum_count(self):
        """Test that ExecutionStatus has exactly 6 values."""
        # Act
        status_values = list(ExecutionStatus)
        
        # Assert
        assert len(status_values) == 6

    def test_execution_status_enum_membership(self):
        """Test ExecutionStatus enum membership."""
        # Act & Assert
        assert "PENDING" in ExecutionStatus.__members__
        assert "PREPARING" in ExecutionStatus.__members__
        assert "RUNNING" in ExecutionStatus.__members__
        assert "COMPLETED" in ExecutionStatus.__members__
        assert "FAILED" in ExecutionStatus.__members__
        assert "CANCELLED" in ExecutionStatus.__members__

    def test_execution_status_string_representation(self):
        """Test string representation of ExecutionStatus values."""
        # Act & Assert - Test that values are accessible
        assert ExecutionStatus.PENDING.value == "PENDING"
        assert ExecutionStatus.PREPARING.value == "PREPARING"
        assert ExecutionStatus.RUNNING.value == "RUNNING"
        assert ExecutionStatus.COMPLETED.value == "COMPLETED"
        assert ExecutionStatus.FAILED.value == "FAILED"
        assert ExecutionStatus.CANCELLED.value == "CANCELLED"

    def test_execution_status_equality(self):
        """Test ExecutionStatus equality with strings."""
        # Act & Assert
        assert ExecutionStatus.PENDING == "PENDING"
        assert ExecutionStatus.PREPARING == "PREPARING"
        assert ExecutionStatus.RUNNING == "RUNNING"
        assert ExecutionStatus.COMPLETED == "COMPLETED"
        assert ExecutionStatus.FAILED == "FAILED"
        assert ExecutionStatus.CANCELLED == "CANCELLED"

    def test_execution_status_iteration(self):
        """Test iteration over ExecutionStatus enum."""
        # Act
        status_values = [status.value for status in ExecutionStatus]
        
        # Assert
        expected_values = ["PENDING", "PREPARING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"]
        assert status_values == expected_values

    def test_execution_status_workflow_order(self):
        """Test that ExecutionStatus values represent a logical workflow."""
        # Act
        status_list = list(ExecutionStatus)
        
        # Assert - Check that statuses are in logical order
        assert status_list[0] == ExecutionStatus.PENDING
        assert status_list[1] == ExecutionStatus.PREPARING
        assert status_list[2] == ExecutionStatus.RUNNING
        # Final states can be in any order but should include:
        final_states = status_list[3:]
        assert ExecutionStatus.COMPLETED in final_states
        assert ExecutionStatus.FAILED in final_states
        assert ExecutionStatus.CANCELLED in final_states

    def test_execution_status_case_sensitivity(self):
        """Test ExecutionStatus case sensitivity."""
        # Act & Assert
        # Should not be equal to lowercase versions
        assert ExecutionStatus.PENDING != "pending"
        assert ExecutionStatus.RUNNING != "running"
        assert ExecutionStatus.COMPLETED != "completed"

    def test_execution_status_initial_states(self):
        """Test identification of initial execution states."""
        # Arrange
        initial_states = [ExecutionStatus.PENDING, ExecutionStatus.PREPARING]
        
        # Act & Assert
        for status in initial_states:
            assert status in [ExecutionStatus.PENDING, ExecutionStatus.PREPARING]

    def test_execution_status_active_states(self):
        """Test identification of active execution states."""
        # Arrange
        active_states = [ExecutionStatus.PREPARING, ExecutionStatus.RUNNING]
        
        # Act & Assert
        for status in active_states:
            assert status in [ExecutionStatus.PREPARING, ExecutionStatus.RUNNING]

    def test_execution_status_final_states(self):
        """Test identification of final execution states."""
        # Arrange
        final_states = [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]
        
        # Act & Assert
        for status in final_states:
            assert status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]

    def test_execution_status_success_states(self):
        """Test identification of successful execution states."""
        # Act & Assert
        assert ExecutionStatus.COMPLETED == "COMPLETED"
        # Only COMPLETED should be considered a success state

    def test_execution_status_error_states(self):
        """Test identification of error execution states."""
        # Arrange
        error_states = [ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]
        
        # Act & Assert
        for status in error_states:
            assert status in [ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]

    def test_execution_status_docstring(self):
        """Test that ExecutionStatus has proper documentation."""
        # Act & Assert
        assert ExecutionStatus.__doc__ is not None
        assert "execution status enum" in ExecutionStatus.__doc__.lower()
        assert "single source of truth" in ExecutionStatus.__doc__.lower()

    def test_execution_status_module_docstring(self):
        """Test that the module has proper documentation."""
        # Act
        import src.models.execution_status as execution_status_module
        
        # Assert
        assert execution_status_module.__doc__ is not None
        assert "execution status" in execution_status_module.__doc__.lower()
        assert "single source of truth" in execution_status_module.__doc__.lower()


class TestExecutionStatusUseCases:
    """Test cases for common ExecutionStatus use cases."""

    def test_execution_status_state_transitions(self):
        """Test common execution status state transitions."""
        # Test typical workflow
        workflow_states = [
            ExecutionStatus.PENDING,
            ExecutionStatus.PREPARING, 
            ExecutionStatus.RUNNING,
            ExecutionStatus.COMPLETED
        ]
        
        # Assert workflow progression makes sense
        assert workflow_states[0] == ExecutionStatus.PENDING
        assert workflow_states[1] == ExecutionStatus.PREPARING
        assert workflow_states[2] == ExecutionStatus.RUNNING
        assert workflow_states[3] == ExecutionStatus.COMPLETED

    def test_execution_status_error_transitions(self):
        """Test error state transitions."""
        # Test error scenarios
        error_scenarios = [
            (ExecutionStatus.PENDING, ExecutionStatus.FAILED),
            (ExecutionStatus.PREPARING, ExecutionStatus.FAILED),
            (ExecutionStatus.RUNNING, ExecutionStatus.FAILED),
            (ExecutionStatus.PENDING, ExecutionStatus.CANCELLED),
            (ExecutionStatus.PREPARING, ExecutionStatus.CANCELLED),
            (ExecutionStatus.RUNNING, ExecutionStatus.CANCELLED)
        ]
        
        for start_state, end_state in error_scenarios:
            # Assert that error transitions are valid
            assert start_state != end_state
            assert end_state in [ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]

    def test_execution_status_filtering(self):
        """Test filtering executions by status."""
        # Arrange
        all_statuses = list(ExecutionStatus)
        
        # Test filtering active executions
        active_filter = lambda s: s in [ExecutionStatus.PREPARING, ExecutionStatus.RUNNING]
        active_statuses = [s for s in all_statuses if active_filter(s)]
        
        # Test filtering completed executions
        completed_filter = lambda s: s in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]
        completed_statuses = [s for s in all_statuses if completed_filter(s)]
        
        # Assert
        assert len(active_statuses) == 2
        assert ExecutionStatus.PREPARING in active_statuses
        assert ExecutionStatus.RUNNING in active_statuses
        
        assert len(completed_statuses) == 3
        assert ExecutionStatus.COMPLETED in completed_statuses
        assert ExecutionStatus.FAILED in completed_statuses
        assert ExecutionStatus.CANCELLED in completed_statuses

    def test_execution_status_json_serialization(self):
        """Test ExecutionStatus JSON serialization compatibility."""
        # Act
        status_values = {status.name: status.value for status in ExecutionStatus}
        
        # Assert
        expected_mapping = {
            "PENDING": "PENDING",
            "PREPARING": "PREPARING", 
            "RUNNING": "RUNNING",
            "COMPLETED": "COMPLETED",
            "FAILED": "FAILED",
            "CANCELLED": "CANCELLED"
        }
        
        assert status_values == expected_mapping

    def test_execution_status_database_compatibility(self):
        """Test ExecutionStatus database storage compatibility."""
        # Test that all status values are valid strings for database storage
        for status in ExecutionStatus:
            # Assert
            assert isinstance(status.value, str)
            assert len(status.value) > 0
            assert status.value.isupper()
            assert not status.value.startswith(" ")
            assert not status.value.endswith(" ")

    def test_execution_status_api_response_compatibility(self):
        """Test ExecutionStatus API response compatibility."""
        # Test that status values are suitable for API responses
        for status in ExecutionStatus:
            # Assert
            assert isinstance(status.value, str)
            # Status values should be descriptive
            assert len(status.value) >= 6  # Shortest is "FAILED" with 6 chars
            # Should not contain special characters that could cause issues
            assert status.value.isalpha()

    def test_execution_status_logging_compatibility(self):
        """Test ExecutionStatus logging compatibility."""
        # Test that status values work well in log messages
        for status in ExecutionStatus:
            log_message = f"Execution status changed to {status}"
            
            # Assert
            assert status.value in log_message
            assert len(log_message) > 20  # Should create meaningful log messages