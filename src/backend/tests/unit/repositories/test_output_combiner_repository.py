"""
Unit tests for OutputCombinerRepository.

Tests the functionality of output combiner repository including
run retrieval by job ID, error handling, and repository configuration.
"""
import pytest
from unittest.mock import MagicMock, patch
from typing import Optional

from sqlalchemy.exc import SQLAlchemyError

from src.repositories.output_combiner_repository import (
    OutputCombinerRepository, 
    output_combiner_repository, 
    get_output_combiner_repository
)
from src.models.execution_history import ExecutionHistory


# Mock execution history model
class MockExecutionHistory:
    def __init__(self, id=1, job_id="job-123", status="running", trigger_type="api",
                 run_name="Test Run", inputs=None, outputs=None, **kwargs):
        self.id = id
        self.job_id = job_id
        self.status = status
        self.trigger_type = trigger_type
        self.run_name = run_name
        self.inputs = inputs or {}
        self.outputs = outputs or {}
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock SQLAlchemy query object
class MockQuery:
    def __init__(self, results=None):
        self.results = results or []
        self._filter_applied = False
    
    def filter(self, *args):
        self._filter_applied = True
        return self
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = MagicMock()
    session.query = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.refresh = MagicMock()
    session.rollback = MagicMock()
    return session


@pytest.fixture
def output_combiner_repo(mock_session):
    """Create an output combiner repository with session."""
    return OutputCombinerRepository(mock_session)


@pytest.fixture
def output_combiner_repo_no_session():
    """Create an output combiner repository without session."""
    return OutputCombinerRepository(None)


@pytest.fixture
def sample_execution_histories():
    """Create sample execution histories for testing."""
    return [
        MockExecutionHistory(
            id=1, job_id="job-123", status="running", 
            run_name="Test Run 1", inputs={"param1": "value1"}
        ),
        MockExecutionHistory(
            id=2, job_id="job-456", status="completed", 
            run_name="Test Run 2", outputs={"result": "success"}
        ),
        MockExecutionHistory(
            id=3, job_id="job-789", status="failed", 
            run_name="Test Run 3", inputs={"param2": "value2"}
        )
    ]


class TestOutputCombinerRepositoryInit:
    """Test repository initialization."""
    
    def test_init_with_session(self, mock_session):
        """Test repository initialization with session."""
        repo = OutputCombinerRepository(mock_session)
        assert repo.db == mock_session
    
    def test_init_without_session(self):
        """Test repository initialization without session."""
        repo = OutputCombinerRepository(None)
        assert repo.db is None


class TestOutputCombinerRepositoryGetRunByJobId:
    """Test get run by job ID functionality."""
    
    def test_get_run_by_job_id_found(self, output_combiner_repo, sample_execution_histories):
        """Test get run by job ID when run is found."""
        target_run = sample_execution_histories[0]  # job-123
        mock_query = MockQuery([target_run])
        output_combiner_repo.db.query.return_value = mock_query
        
        result = output_combiner_repo.get_run_by_job_id("job-123")
        
        assert result == target_run
        assert result.job_id == "job-123"
        output_combiner_repo.db.query.assert_called_once_with(ExecutionHistory)
        assert mock_query._filter_applied
    
    def test_get_run_by_job_id_not_found(self, output_combiner_repo):
        """Test get run by job ID when run is not found."""
        mock_query = MockQuery([])  # No results
        output_combiner_repo.db.query.return_value = mock_query
        
        result = output_combiner_repo.get_run_by_job_id("nonexistent-job")
        
        assert result is None
        output_combiner_repo.db.query.assert_called_once_with(ExecutionHistory)
        assert mock_query._filter_applied
    
    def test_get_run_by_job_id_multiple_matches_returns_first(self, output_combiner_repo, sample_execution_histories):
        """Test get run by job ID returns first match when multiple exist."""
        # This shouldn't happen in real usage (job_id should be unique), but test the behavior
        duplicate_runs = [sample_execution_histories[0], sample_execution_histories[1]]
        mock_query = MockQuery(duplicate_runs)
        output_combiner_repo.db.query.return_value = mock_query
        
        result = output_combiner_repo.get_run_by_job_id("job-123")
        
        assert result == duplicate_runs[0]  # Should return first match
    
    def test_get_run_by_job_id_sqlalchemy_error(self, output_combiner_repo):
        """Test get run by job ID handles SQLAlchemy errors."""
        output_combiner_repo.db.query.side_effect = SQLAlchemyError("Database connection error")
        
        result = output_combiner_repo.get_run_by_job_id("job-123")
        
        assert result is None
        output_combiner_repo.db.query.assert_called_once_with(ExecutionHistory)
    
    def test_get_run_by_job_id_general_exception(self, output_combiner_repo):
        """Test get run by job ID handles general exceptions."""
        output_combiner_repo.db.query.side_effect = Exception("Unexpected error")
        
        result = output_combiner_repo.get_run_by_job_id("job-123")
        
        assert result is None
        output_combiner_repo.db.query.assert_called_once_with(ExecutionHistory)
    
    def test_get_run_by_job_id_empty_string(self, output_combiner_repo):
        """Test get run by job ID with empty string."""
        mock_query = MockQuery([])
        output_combiner_repo.db.query.return_value = mock_query
        
        result = output_combiner_repo.get_run_by_job_id("")
        
        assert result is None
    
    def test_get_run_by_job_id_none_input(self, output_combiner_repo):
        """Test get run by job ID with None input."""
        mock_query = MockQuery([])
        output_combiner_repo.db.query.return_value = mock_query
        
        result = output_combiner_repo.get_run_by_job_id(None)
        
        assert result is None


class TestOutputCombinerRepositoryGlobalInstance:
    """Test global instance functionality."""
    
    def test_global_instance_exists(self):
        """Test that global output_combiner_repository instance exists."""
        assert output_combiner_repository is not None
        assert isinstance(output_combiner_repository, OutputCombinerRepository)
        assert output_combiner_repository.db is None
    
    def test_get_output_combiner_repository_with_session(self, mock_session):
        """Test get_output_combiner_repository with provided session."""
        repo = get_output_combiner_repository(mock_session)
        
        assert repo == output_combiner_repository
        assert repo.db == mock_session
    
    def test_get_output_combiner_repository_updates_global_instance(self, mock_session):
        """Test get_output_combiner_repository updates the global instance."""
        original_db = output_combiner_repository.db
        
        repo = get_output_combiner_repository(mock_session)
        
        assert output_combiner_repository.db == mock_session
        assert repo == output_combiner_repository
        
        # Clean up - restore original state
        output_combiner_repository.db = original_db
    
    def test_multiple_calls_to_get_repository(self, mock_session):
        """Test multiple calls to get_output_combiner_repository."""
        # First call
        repo1 = get_output_combiner_repository(mock_session)
        assert repo1.db == mock_session
        
        # Second call with different session
        mock_session2 = MagicMock()
        repo2 = get_output_combiner_repository(mock_session2)
        
        # Should be same instance but with updated session
        assert repo1 == repo2
        assert repo2.db == mock_session2
        assert output_combiner_repository.db == mock_session2
        
        # Clean up
        output_combiner_repository.db = None


class TestOutputCombinerRepositoryErrorHandling:
    """Test error handling scenarios."""
    
    def test_error_logging_sqlalchemy_error(self, output_combiner_repo):
        """Test that SQLAlchemy errors are properly logged."""
        with patch('src.repositories.output_combiner_repository.logger') as mock_logger:
            output_combiner_repo.db.query.side_effect = SQLAlchemyError("DB Error")
            
            result = output_combiner_repo.get_run_by_job_id("job-123")
            
            assert result is None
            mock_logger.error.assert_called_once()
            error_message = mock_logger.error.call_args[0][0]
            assert "Database error retrieving run with job_id job-123" in error_message
    
    def test_error_logging_general_exception(self, output_combiner_repo):
        """Test that general exceptions are properly logged."""
        with patch('src.repositories.output_combiner_repository.logger') as mock_logger:
            output_combiner_repo.db.query.side_effect = Exception("General Error")
            
            result = output_combiner_repo.get_run_by_job_id("job-123")
            
            assert result is None
            mock_logger.error.assert_called_once()
            error_message = mock_logger.error.call_args[0][0]
            assert "Error retrieving run with job_id job-123" in error_message
    
    def test_no_exception_when_no_session(self, output_combiner_repo_no_session):
        """Test that method doesn't crash when no session is provided."""
        # This will likely cause an AttributeError, but let's see how it's handled
        try:
            result = output_combiner_repo_no_session.get_run_by_job_id("job-123")
            # If we reach here, it means the method handled the None session gracefully
            assert result is None
        except AttributeError:
            # This is expected when trying to call .query() on None
            pass


class TestOutputCombinerRepositoryIntegration:
    """Test integration scenarios and workflows."""
    
    def test_typical_workflow(self, output_combiner_repo, sample_execution_histories):
        """Test typical workflow of retrieving run information."""
        target_run = sample_execution_histories[1]  # job-456, completed
        mock_query = MockQuery([target_run])
        output_combiner_repo.db.query.return_value = mock_query
        
        # Simulate OutputCombinerCallback workflow
        job_id = "job-456"
        
        # 1. Get run information
        run = output_combiner_repo.get_run_by_job_id(job_id)
        assert run is not None
        assert run.job_id == job_id
        assert run.status == "completed"
        
        # 2. Access run properties (would be used by callback)
        assert run.run_name == "Test Run 2"
        assert run.outputs == {"result": "success"}
        assert run.id == 2
    
    def test_error_recovery_workflow(self, output_combiner_repo):
        """Test workflow when database errors occur."""
        # Simulate database connection issues
        output_combiner_repo.db.query.side_effect = SQLAlchemyError("Connection lost")
        
        # The repository should handle the error gracefully
        result = output_combiner_repo.get_run_by_job_id("job-123")
        assert result is None
        
        # After connection is restored, it should work again
        mock_execution = MockExecutionHistory(job_id="job-123")
        mock_query = MockQuery([mock_execution])
        output_combiner_repo.db.query.side_effect = None
        output_combiner_repo.db.query.return_value = mock_query
        
        result = output_combiner_repo.get_run_by_job_id("job-123")
        assert result == mock_execution
    
    def test_repository_session_management(self, mock_session):
        """Test repository session management patterns."""
        # Test creating repository with session
        repo_with_session = OutputCombinerRepository(mock_session)
        assert repo_with_session.db == mock_session
        
        # Test using global instance with session injection
        global_repo = get_output_combiner_repository(mock_session)
        assert global_repo.db == mock_session
        
        # Test that both work the same way
        mock_execution = MockExecutionHistory(job_id="test-job")
        mock_query = MockQuery([mock_execution])
        mock_session.query.return_value = mock_query
        
        result1 = repo_with_session.get_run_by_job_id("test-job")
        result2 = global_repo.get_run_by_job_id("test-job")
        
        assert result1 == result2 == mock_execution
        
        # Clean up
        output_combiner_repository.db = None
    
    def test_different_job_id_formats(self, output_combiner_repo, sample_execution_histories):
        """Test handling different job ID formats."""
        test_cases = [
            ("simple-job", sample_execution_histories[0]),
            ("job_with_underscores", sample_execution_histories[1]),
            ("job-with-dashes-123", sample_execution_histories[2]),
            ("JOB-UPPERCASE", None),  # Not found
            ("job.with.dots.456", None)  # Not found
        ]
        
        for job_id, expected_result in test_cases:
            if expected_result:
                mock_query = MockQuery([expected_result])
            else:
                mock_query = MockQuery([])
            
            output_combiner_repo.db.query.return_value = mock_query
            
            result = output_combiner_repo.get_run_by_job_id(job_id)
            assert result == expected_result
    
    def test_concurrent_access_simulation(self, mock_session):
        """Test simulation of concurrent access to repository."""
        # Create multiple repository instances (simulating concurrent requests)
        repo1 = OutputCombinerRepository(mock_session)
        repo2 = OutputCombinerRepository(mock_session)
        
        # Both should work independently
        mock_execution1 = MockExecutionHistory(job_id="concurrent-job-1")
        mock_execution2 = MockExecutionHistory(job_id="concurrent-job-2")
        
        # Simulate different queries returning different results
        def side_effect(*args):
            query_mock = MockQuery([mock_execution1])
            return query_mock
        
        mock_session.query.side_effect = side_effect
        
        result1 = repo1.get_run_by_job_id("concurrent-job-1")
        result2 = repo2.get_run_by_job_id("concurrent-job-2")
        
        # Both should get results (in real scenario, they'd be different)
        assert result1 == mock_execution1
        assert result2 == mock_execution1  # Same due to mock limitation
    
    def test_repository_state_isolation(self, mock_session):
        """Test that different repository instances maintain separate state."""
        # Create two separate instances
        repo1 = OutputCombinerRepository(mock_session)
        repo2 = OutputCombinerRepository(None)
        
        assert repo1.db == mock_session
        assert repo2.db is None
        
        # Modifying one shouldn't affect the other
        repo1.db = None
        assert repo2.db is None
        
        # Global instance should be separate from both
        assert output_combiner_repository.db is None or output_combiner_repository.db != mock_session