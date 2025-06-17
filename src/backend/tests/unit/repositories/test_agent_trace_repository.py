"""
Unit tests for AgentTraceRepository.

Tests the functionality of agent trace repository including
async/sync trace creation, session management, error handling, and trace recording.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, UTC
from typing import Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.agent_trace_repository import AgentTraceRepository, agent_trace_repository, get_agent_trace_repository
from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace


# Mock execution history model
class MockExecutionHistory:
    def __init__(self, id=1, job_id="job-123", status="running", **kwargs):
        self.id = id
        self.job_id = job_id
        self.status = status
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock execution trace model
class MockExecutionTrace:
    def __init__(self, id=1, run_id=1, job_id="job-123", agent_name="test-agent",
                 task_name="test-task", event_type="agent_step", output="test output",
                 trace_metadata=None, created_at=None, **kwargs):
        self.id = id
        self.run_id = run_id
        self.job_id = job_id
        self.agent_name = agent_name
        self.task_name = task_name
        self.event_type = event_type
        self.output = output
        self.trace_metadata = trace_metadata or {}
        self.created_at = created_at or datetime.now(UTC)
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results=None, scalar_value=None, rowcount=0):
        self._scalars = MockScalars(results or [])
        self._scalar_value = scalar_value
        self.rowcount = rowcount
    
    def scalars(self):
        return self._scalars
    
    def scalar(self):
        return self._scalar_value


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.query = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.refresh = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    return session


@pytest.fixture
def agent_trace_repo():
    """Create an agent trace repository without a session."""
    return AgentTraceRepository()


@pytest.fixture
def agent_trace_repo_with_async_session(mock_async_session):
    """Create an agent trace repository with async session."""
    return AgentTraceRepository(db=mock_async_session)


@pytest.fixture
def agent_trace_repo_with_sync_session(mock_sync_session):
    """Create an agent trace repository with sync session."""
    return AgentTraceRepository(db=mock_sync_session)


@pytest.fixture
def sample_execution_history():
    """Create sample execution history for testing."""
    return MockExecutionHistory(id=123, job_id="job-123", status="running")


@pytest.fixture
def sample_trace_data():
    """Create sample trace data for testing."""
    return {
        "job_id": "job-123",
        "agent_name": "test-agent",
        "task_name": "test-task",
        "event_type": "agent_step",
        "content": "test output content",
        "timestamp": "2023-01-01T12:00:00Z",
        "trace_metadata": {"key": "value"}
    }


class TestAgentTraceRepositoryInit:
    """Test repository initialization."""
    
    def test_init_without_session(self):
        """Test repository initialization without session."""
        repo = AgentTraceRepository()
        assert repo.db is None
    
    def test_init_with_session(self, mock_async_session):
        """Test repository initialization with session."""
        repo = AgentTraceRepository(db=mock_async_session)
        assert repo.db == mock_async_session


class TestAgentTraceRepositoryCreateTraceAsync:
    """Test async create_trace functionality."""
    
    @pytest.mark.asyncio
    async def test_create_trace_success(self, agent_trace_repo_with_async_session, 
                                       sample_execution_history, sample_trace_data):
        """Test successful async trace creation."""
        # Mock finding execution history
        mock_result = MockResult([sample_execution_history])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        # Mock ExecutionTrace creation
        created_trace = MockExecutionTrace(
            id=456,
            run_id=sample_execution_history.id,
            **{k: v for k, v in sample_trace_data.items() if k != 'content'}
        )
        created_trace.output = sample_trace_data['content']
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = created_trace
            
            result = await agent_trace_repo_with_async_session.create_trace(**sample_trace_data)
            
            assert result == created_trace.id
            agent_trace_repo_with_async_session.db.add.assert_called_once_with(created_trace)
            
            # Verify ExecutionTrace was created with correct parameters
            call_args = mock_trace.call_args[1]
            assert call_args['run_id'] == sample_execution_history.id
            assert call_args['job_id'] == sample_trace_data['job_id']
            assert call_args['agent_name'] == sample_trace_data['agent_name']
            assert call_args['task_name'] == sample_trace_data['task_name']
            assert call_args['event_type'] == sample_trace_data['event_type']
            assert call_args['output'] == sample_trace_data['content']
            assert call_args['trace_metadata'] == sample_trace_data['trace_metadata']
    
    @pytest.mark.asyncio
    async def test_create_trace_with_valid_timestamp(self, agent_trace_repo_with_async_session, 
                                                    sample_execution_history):
        """Test create trace with valid ISO timestamp."""
        mock_result = MockResult([sample_execution_history])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        iso_timestamp = "2023-01-01T12:00:00Z"
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = MockExecutionTrace()
            
            await agent_trace_repo_with_async_session.create_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                event_type="agent_step",
                content="test content",
                timestamp=iso_timestamp
            )
            
            # Verify timestamp was parsed correctly
            call_args = mock_trace.call_args[1]
            expected_dt = datetime.fromisoformat(iso_timestamp)
            assert call_args['created_at'] == expected_dt
    
    @pytest.mark.asyncio
    async def test_create_trace_with_invalid_timestamp(self, agent_trace_repo_with_async_session, 
                                                      sample_execution_history):
        """Test create trace with invalid timestamp falls back to current time."""
        mock_result = MockResult([sample_execution_history])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = MockExecutionTrace()
            
            with patch('src.repositories.agent_trace_repository.datetime') as mock_datetime:
                now_time = datetime.now(UTC)
                mock_datetime.fromisoformat.side_effect = ValueError("Invalid format")
                mock_datetime.now.return_value = now_time
                mock_datetime.UTC = UTC
                
                await agent_trace_repo_with_async_session.create_trace(
                    job_id="job-123",
                    agent_name="test-agent",
                    task_name="test-task",
                    event_type="agent_step",
                    content="test content",
                    timestamp="invalid-timestamp"
                )
                
                # Verify fallback to current time
                call_args = mock_trace.call_args[1]
                assert call_args['created_at'] == now_time
    
    @pytest.mark.asyncio
    async def test_create_trace_without_timestamp(self, agent_trace_repo_with_async_session, 
                                                 sample_execution_history):
        """Test create trace without timestamp uses current time."""
        mock_result = MockResult([sample_execution_history])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = MockExecutionTrace()
            
            with patch('src.repositories.agent_trace_repository.datetime') as mock_datetime:
                now_time = datetime.now(UTC)
                mock_datetime.now.return_value = now_time
                mock_datetime.UTC = UTC
                
                await agent_trace_repo_with_async_session.create_trace(
                    job_id="job-123",
                    agent_name="test-agent",
                    task_name="test-task",
                    event_type="agent_step",
                    content="test content"
                )
                
                # Verify current time was used
                call_args = mock_trace.call_args[1]
                assert call_args['created_at'] == now_time
    
    @pytest.mark.asyncio
    async def test_create_trace_execution_history_not_found(self, agent_trace_repo_with_async_session):
        """Test create trace when execution history is not found."""
        # Mock empty result (no execution history found)
        mock_result = MockResult([])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        with pytest.raises(ValueError, match="No execution record found for job_id: job-123"):
            await agent_trace_repo_with_async_session.create_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                event_type="agent_step",
                content="test content"
            )
    
    @pytest.mark.asyncio
    async def test_create_trace_non_async_session_error(self, mock_sync_session):
        """Test create trace with non-async session raises error."""
        repo = AgentTraceRepository(db=mock_sync_session)
        
        with pytest.raises(ValueError, match="Database session must be AsyncSession"):
            await repo.create_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                event_type="agent_step",
                content="test content"
            )
    
    @pytest.mark.asyncio
    async def test_create_trace_database_error(self, agent_trace_repo_with_async_session):
        """Test create trace handles database errors."""
        # Mock database error during execute
        agent_trace_repo_with_async_session.db.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await agent_trace_repo_with_async_session.create_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                event_type="agent_step",
                content="test content"
            )
    
    @pytest.mark.asyncio
    async def test_create_trace_with_default_metadata(self, agent_trace_repo_with_async_session, 
                                                     sample_execution_history):
        """Test create trace with default empty metadata."""
        mock_result = MockResult([sample_execution_history])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = MockExecutionTrace()
            
            await agent_trace_repo_with_async_session.create_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                event_type="agent_step",
                content="test content"
            )
            
            # Verify default empty metadata
            call_args = mock_trace.call_args[1]
            assert call_args['trace_metadata'] == {}


class TestAgentTraceRepositoryRecordTraceSync:
    """Test sync record_trace functionality."""
    
    def test_record_trace_with_provided_session(self, agent_trace_repo_with_sync_session, 
                                               sample_execution_history):
        """Test record trace with provided sync session."""
        # Mock query result
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_first = MagicMock(return_value=sample_execution_history)
        
        agent_trace_repo_with_sync_session.db.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.first.return_value = sample_execution_history
        
        # Mock ExecutionTrace creation
        created_trace = MockExecutionTrace(
            id=456,
            run_id=sample_execution_history.id,
            job_id="job-123",
            agent_name="test-agent",
            task_name="test-task",
            output="test output"
        )
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = created_trace
            
            result = agent_trace_repo_with_sync_session.record_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                output_content="test output"
            )
            
            assert result == created_trace
            agent_trace_repo_with_sync_session.db.add.assert_called_once_with(created_trace)
            agent_trace_repo_with_sync_session.db.commit.assert_called_once()
            agent_trace_repo_with_sync_session.db.refresh.assert_called_once_with(created_trace)
            
            # Verify session was not closed (not created by method)
            agent_trace_repo_with_sync_session.db.close.assert_not_called()
    
    def test_record_trace_without_session_creates_new_session(self, agent_trace_repo, 
                                                             sample_execution_history):
        """Test record trace without session creates new session."""
        with patch('src.repositories.agent_trace_repository.SessionLocal') as mock_session_factory:
            mock_session = MagicMock()
            mock_session_factory.return_value = mock_session
            
            # Mock query result
            mock_query = MagicMock()
            mock_filter = MagicMock()
            mock_first = MagicMock(return_value=sample_execution_history)
            
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_filter
            mock_filter.first.return_value = sample_execution_history
            
            created_trace = MockExecutionTrace()
            
            with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
                mock_trace.return_value = created_trace
                
                result = agent_trace_repo.record_trace(
                    job_id="job-123",
                    agent_name="test-agent",
                    task_name="test-task",
                    output_content="test output"
                )
                
                assert result == created_trace
                mock_session.add.assert_called_once_with(created_trace)
                mock_session.commit.assert_called_once()
                mock_session.refresh.assert_called_once_with(created_trace)
                
                # Verify session was closed (created by method)
                mock_session.close.assert_called_once()
    
    def test_record_trace_execution_history_not_found(self, agent_trace_repo_with_sync_session):
        """Test record trace when execution history is not found."""
        # Mock query to return None
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_first = MagicMock(return_value=None)
        
        agent_trace_repo_with_sync_session.db.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.first.return_value = None
        
        result = agent_trace_repo_with_sync_session.record_trace(
            job_id="job-123",
            agent_name="test-agent",
            task_name="test-task",
            output_content="test output"
        )
        
        assert result is None
        # Verify no add/commit was called
        agent_trace_repo_with_sync_session.db.add.assert_not_called()
        agent_trace_repo_with_sync_session.db.commit.assert_not_called()
    
    def test_record_trace_database_error(self, agent_trace_repo_with_sync_session, 
                                        sample_execution_history):
        """Test record trace handles database errors."""
        # Mock query result
        mock_query = MagicMock()
        mock_filter = MagicMock()
        mock_first = MagicMock(return_value=sample_execution_history)
        
        agent_trace_repo_with_sync_session.db.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.first.return_value = sample_execution_history
        
        # Mock database error during commit
        agent_trace_repo_with_sync_session.db.commit.side_effect = SQLAlchemyError("Commit failed")
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace'):
            result = agent_trace_repo_with_sync_session.record_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                output_content="test output"
            )
            
            assert result is None
            agent_trace_repo_with_sync_session.db.rollback.assert_called_once()
    
    def test_record_trace_general_exception(self, agent_trace_repo):
        """Test record trace handles general exceptions."""
        with patch('src.repositories.agent_trace_repository.SessionLocal') as mock_session_factory:
            # Mock exception during session creation
            mock_session_factory.side_effect = Exception("Session creation failed")
            
            result = agent_trace_repo.record_trace(
                job_id="job-123",
                agent_name="test-agent",
                task_name="test-task",
                output_content="test output"
            )
            
            assert result is None
    
    def test_record_trace_session_cleanup_on_exception(self, agent_trace_repo, 
                                                      sample_execution_history):
        """Test record trace cleans up session on exception."""
        with patch('src.repositories.agent_trace_repository.SessionLocal') as mock_session_factory:
            mock_session = MagicMock()
            mock_session_factory.return_value = mock_session
            
            # Mock query result
            mock_query = MagicMock()
            mock_filter = MagicMock()
            mock_first = MagicMock(return_value=sample_execution_history)
            
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_filter
            mock_filter.first.return_value = sample_execution_history
            
            # Mock exception during add
            mock_session.add.side_effect = Exception("Add failed")
            
            with patch('src.repositories.agent_trace_repository.ExecutionTrace'):
                result = agent_trace_repo.record_trace(
                    job_id="job-123",
                    agent_name="test-agent",
                    task_name="test-task",
                    output_content="test output"
                )
                
                assert result is None
                # Verify session was still closed despite exception
                mock_session.close.assert_called_once()


class TestAgentTraceRepositoryUtilityFunctions:
    """Test utility functions and global instances."""
    
    def test_global_agent_trace_repository_instance(self):
        """Test global agent_trace_repository instance."""
        assert agent_trace_repository is not None
        assert isinstance(agent_trace_repository, AgentTraceRepository)
        assert agent_trace_repository.db is None
    
    def test_get_agent_trace_repository_without_session(self):
        """Test get_agent_trace_repository without providing session."""
        repo = get_agent_trace_repository()
        assert repo == agent_trace_repository
        assert repo.db is None
    
    def test_get_agent_trace_repository_with_session(self, mock_async_session):
        """Test get_agent_trace_repository with provided session."""
        repo = get_agent_trace_repository(db=mock_async_session)
        assert repo == agent_trace_repository
        assert repo.db == mock_async_session
        
        # Clean up - reset to None for other tests
        agent_trace_repository.db = None
    
    def test_get_agent_trace_repository_updates_existing_instance(self, mock_sync_session):
        """Test get_agent_trace_repository updates the existing global instance."""
        original_db = agent_trace_repository.db
        
        repo = get_agent_trace_repository(db=mock_sync_session)
        assert repo == agent_trace_repository
        assert agent_trace_repository.db == mock_sync_session
        
        # Clean up - restore original state
        agent_trace_repository.db = original_db


class TestAgentTraceRepositoryIntegration:
    """Test integration scenarios and workflows."""
    
    @pytest.mark.asyncio
    async def test_async_create_trace_full_workflow(self, agent_trace_repo_with_async_session, 
                                                   sample_execution_history):
        """Test complete async create trace workflow."""
        # Mock finding execution history
        mock_result = MockResult([sample_execution_history])
        agent_trace_repo_with_async_session.db.execute.return_value = mock_result
        
        # Create realistic trace data
        trace_data = {
            "job_id": "job-123",
            "agent_name": "researcher-agent",
            "task_name": "data-analysis",
            "event_type": "tool_start",
            "content": "Starting data analysis with pandas",
            "timestamp": "2023-01-01T12:00:00Z",
            "trace_metadata": {
                "tool_name": "pandas",
                "operation": "read_csv",
                "file_size": 1024
            }
        }
        
        created_trace = MockExecutionTrace(
            id=789,
            run_id=sample_execution_history.id,
            job_id=trace_data["job_id"],
            agent_name=trace_data["agent_name"],
            task_name=trace_data["task_name"],
            event_type=trace_data["event_type"],
            output=trace_data["content"],
            trace_metadata=trace_data["trace_metadata"]
        )
        
        with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
            mock_trace.return_value = created_trace
            
            result = await agent_trace_repo_with_async_session.create_trace(**trace_data)
            
            assert result == created_trace.id
            
            # Verify all parameters were passed correctly
            call_args = mock_trace.call_args[1]
            assert call_args['run_id'] == sample_execution_history.id
            assert call_args['job_id'] == trace_data['job_id']
            assert call_args['agent_name'] == trace_data['agent_name']
            assert call_args['task_name'] == trace_data['task_name']
            assert call_args['event_type'] == trace_data['event_type']
            assert call_args['output'] == trace_data['content']
            assert call_args['trace_metadata'] == trace_data['trace_metadata']
            assert isinstance(call_args['created_at'], datetime)
    
    def test_sync_record_trace_full_workflow(self, sample_execution_history):
        """Test complete sync record trace workflow."""
        with patch('src.repositories.agent_trace_repository.SessionLocal') as mock_session_factory:
            mock_session = MagicMock()
            mock_session_factory.return_value = mock_session
            
            # Mock query result
            mock_query = MagicMock()
            mock_filter = MagicMock()
            mock_query.filter.return_value = mock_filter
            mock_filter.first.return_value = sample_execution_history
            mock_session.query.return_value = mock_query
            
            # Create expected trace
            expected_trace = MockExecutionTrace(
                id=999,
                run_id=sample_execution_history.id,
                job_id="job-456",
                agent_name="writer-agent",
                task_name="content-generation",
                output="Generated article content"
            )
            
            with patch('src.repositories.agent_trace_repository.ExecutionTrace') as mock_trace:
                mock_trace.return_value = expected_trace
                
                repo = AgentTraceRepository()
                result = repo.record_trace(
                    job_id="job-456",
                    agent_name="writer-agent",
                    task_name="content-generation",
                    output_content="Generated article content"
                )
                
                assert result == expected_trace
                
                # Verify ExecutionTrace creation
                call_args = mock_trace.call_args[1]
                assert call_args['run_id'] == sample_execution_history.id
                assert call_args['job_id'] == "job-456"
                assert call_args['agent_name'] == "writer-agent"
                assert call_args['task_name'] == "content-generation"
                assert call_args['output'] == "Generated article content"
                assert isinstance(call_args['created_at'], datetime)
                
                # Verify session operations
                mock_session.add.assert_called_once_with(expected_trace)
                mock_session.commit.assert_called_once()
                mock_session.refresh.assert_called_once_with(expected_trace)
                mock_session.close.assert_called_once()
    
    def test_mixed_async_sync_session_handling(self, mock_async_session, mock_sync_session):
        """Test that repository correctly handles different session types."""
        # Test async session
        async_repo = AgentTraceRepository(db=mock_async_session)
        assert async_repo.db == mock_async_session
        
        # Test sync session
        sync_repo = AgentTraceRepository(db=mock_sync_session)
        assert sync_repo.db == mock_sync_session
        
        # Test no session
        no_session_repo = AgentTraceRepository()
        assert no_session_repo.db is None
    
    def test_repository_state_isolation(self, mock_async_session, mock_sync_session):
        """Test that different repository instances maintain separate state."""
        repo1 = AgentTraceRepository(db=mock_async_session)
        repo2 = AgentTraceRepository(db=mock_sync_session)
        repo3 = AgentTraceRepository()
        
        assert repo1.db == mock_async_session
        assert repo2.db == mock_sync_session
        assert repo3.db is None
        
        # Modifying one doesn't affect others
        repo1.db = None
        assert repo2.db == mock_sync_session
        assert repo3.db is None