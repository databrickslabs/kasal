"""
Unit tests for ExecutionTraceService.

Tests the functionality of execution trace operations including
retrieving traces by run_id, job_id, creating traces, and deleting traces.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC
from typing import List, Optional, Dict, Any

from sqlalchemy.exc import SQLAlchemyError

from src.services.execution_trace_service import ExecutionTraceService
from src.schemas.execution_trace import (
    ExecutionTraceItem,
    ExecutionTraceList,
    ExecutionTraceResponseByRunId,
    ExecutionTraceResponseByJobId,
    DeleteTraceResponse
)


# Mock models
class MockExecutionTrace:
    """Mock ExecutionTrace model."""
    def __init__(self, id=1, run_id=1, job_id="job-123", timestamp=None, 
                 created_at=None, event_source="test", event_context="test_context",
                 event_type="test_event", input_data=None, output_data=None, output=None):
        self.id = id
        self.run_id = run_id
        self.job_id = job_id
        self.timestamp = timestamp or datetime.now(UTC)
        self.created_at = created_at or datetime.now(UTC)
        self.event_source = event_source
        self.event_context = event_context
        self.event_type = event_type
        self.input_data = input_data or {"input": "test"}
        self.output_data = output_data or {"output": "test"}
        self.output = output or "test output"


@pytest.fixture
def mock_trace():
    """Create a mock execution trace."""
    return MockExecutionTrace()


@pytest.fixture
def mock_traces():
    """Create a list of mock execution traces."""
    return [
        MockExecutionTrace(id=1, run_id=1, job_id="job-123"),
        MockExecutionTrace(id=2, run_id=1, job_id="job-123"),
        MockExecutionTrace(id=3, run_id=2, job_id="job-456")
    ]


@pytest.fixture
def mock_execution_trace_repository():
    """Mock the execution_trace_repository."""
    with patch('src.services.execution_trace_service.execution_trace_repository') as mock_repo:
        # Set up all repository methods as AsyncMock
        mock_repo.get_execution_job_id_by_run_id = AsyncMock()
        mock_repo.get_by_run_id = AsyncMock()
        mock_repo.get_execution_run_id_by_job_id = AsyncMock()
        mock_repo.get_by_job_id = AsyncMock()
        mock_repo.get_all_traces = AsyncMock()
        mock_repo.get_by_id = AsyncMock()
        mock_repo.create = AsyncMock()
        mock_repo.delete_by_id = AsyncMock()
        mock_repo.delete_by_run_id = AsyncMock()
        mock_repo.delete_by_job_id = AsyncMock()
        mock_repo.delete_all = AsyncMock()
        yield mock_repo


class TestExecutionTraceService:
    """Test cases for ExecutionTraceService."""
    
    @pytest.mark.asyncio
    async def test_get_traces_by_run_id_success(self, mock_execution_trace_repository, mock_traces):
        """Test successful retrieval of traces by run_id."""
        run_id = 1
        job_id = "job-123"
        
        # Mock repository methods
        mock_execution_trace_repository.get_execution_job_id_by_run_id.return_value = job_id
        mock_execution_trace_repository.get_by_run_id.return_value = mock_traces[:2]
        
        result = await ExecutionTraceService.get_traces_by_run_id(
            db=None, run_id=run_id, limit=100, offset=0
        )
        
        assert isinstance(result, ExecutionTraceResponseByRunId)
        assert result.run_id == run_id
        assert len(result.traces) == 2
        assert all(isinstance(trace, ExecutionTraceItem) for trace in result.traces)
        
        mock_execution_trace_repository.get_execution_job_id_by_run_id.assert_called_once_with(run_id)
        mock_execution_trace_repository.get_by_run_id.assert_called_once_with(run_id, 100, 0)
    
    @pytest.mark.asyncio
    async def test_get_traces_by_run_id_execution_not_found(self, mock_execution_trace_repository):
        """Test get_traces_by_run_id when execution doesn't exist."""
        run_id = 999
        
        mock_execution_trace_repository.get_execution_job_id_by_run_id.return_value = None
        
        result = await ExecutionTraceService.get_traces_by_run_id(
            db=None, run_id=run_id, limit=100, offset=0
        )
        
        assert result is None
        mock_execution_trace_repository.get_execution_job_id_by_run_id.assert_called_once_with(run_id)
        mock_execution_trace_repository.get_by_run_id.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_traces_by_run_id_update_missing_job_id(self, mock_execution_trace_repository):
        """Test get_traces_by_run_id updates missing job_id in traces."""
        run_id = 1
        job_id = "job-123"
        
        # Create traces with missing job_id
        traces_with_missing_job_id = [
            MockExecutionTrace(id=1, run_id=run_id, job_id=None),
            MockExecutionTrace(id=2, run_id=run_id, job_id="job-123")
        ]
        
        mock_execution_trace_repository.get_execution_job_id_by_run_id.return_value = job_id
        mock_execution_trace_repository.get_by_run_id.return_value = traces_with_missing_job_id
        
        result = await ExecutionTraceService.get_traces_by_run_id(
            db=None, run_id=run_id, limit=100, offset=0
        )
        
        assert result is not None
        assert len(result.traces) == 2
        # Verify job_id was updated for the trace that was missing it
        assert traces_with_missing_job_id[0].job_id == job_id
        assert traces_with_missing_job_id[1].job_id == "job-123"
    
    @pytest.mark.asyncio
    async def test_get_traces_by_run_id_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test get_traces_by_run_id handles SQLAlchemy errors."""
        run_id = 1
        
        mock_execution_trace_repository.get_execution_job_id_by_run_id.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.get_traces_by_run_id(
                db=None, run_id=run_id, limit=100, offset=0
            )
    
    @pytest.mark.asyncio
    async def test_get_traces_by_run_id_general_error(self, mock_execution_trace_repository):
        """Test get_traces_by_run_id handles general errors."""
        run_id = 1
        
        mock_execution_trace_repository.get_execution_job_id_by_run_id.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.get_traces_by_run_id(
                db=None, run_id=run_id, limit=100, offset=0
            )
    
    @pytest.mark.asyncio
    async def test_get_traces_by_job_id_success(self, mock_execution_trace_repository, mock_traces):
        """Test successful retrieval of traces by job_id."""
        job_id = "job-123"
        run_id = 1
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.return_value = run_id
        mock_execution_trace_repository.get_by_job_id.return_value = mock_traces[:2]
        
        result = await ExecutionTraceService.get_traces_by_job_id(
            db=None, job_id=job_id, limit=100, offset=0
        )
        
        assert isinstance(result, ExecutionTraceResponseByJobId)
        assert result.job_id == job_id
        assert len(result.traces) == 2
        assert all(isinstance(trace, ExecutionTraceItem) for trace in result.traces)
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.assert_called_once_with(job_id)
        mock_execution_trace_repository.get_by_job_id.assert_called_once_with(job_id, 100, 0)
    
    @pytest.mark.asyncio
    async def test_get_traces_by_job_id_execution_not_found(self, mock_execution_trace_repository):
        """Test get_traces_by_job_id when execution doesn't exist."""
        job_id = "nonexistent-job"
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.return_value = None
        
        result = await ExecutionTraceService.get_traces_by_job_id(
            db=None, job_id=job_id, limit=100, offset=0
        )
        
        assert result is None
        mock_execution_trace_repository.get_execution_run_id_by_job_id.assert_called_once_with(job_id)
        mock_execution_trace_repository.get_by_job_id.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_traces_by_job_id_fallback_to_run_id(self, mock_execution_trace_repository, mock_traces):
        """Test get_traces_by_job_id falls back to run_id lookup when no direct traces found."""
        job_id = "job-123"
        run_id = 1
        
        # Create traces with missing job_id
        traces_with_missing_job_id = [
            MockExecutionTrace(id=1, run_id=run_id, job_id=None),
            MockExecutionTrace(id=2, run_id=run_id, job_id=None)
        ]
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.return_value = run_id
        mock_execution_trace_repository.get_by_job_id.return_value = []  # No direct traces
        mock_execution_trace_repository.get_by_run_id.return_value = traces_with_missing_job_id
        
        result = await ExecutionTraceService.get_traces_by_job_id(
            db=None, job_id=job_id, limit=100, offset=0
        )
        
        assert result is not None
        assert len(result.traces) == 2
        # Verify job_id was updated for traces that were missing it
        assert traces_with_missing_job_id[0].job_id == job_id
        assert traces_with_missing_job_id[1].job_id == job_id
        
        mock_execution_trace_repository.get_by_job_id.assert_called_once_with(job_id, 100, 0)
        mock_execution_trace_repository.get_by_run_id.assert_called_once_with(run_id, 100, 0)
    
    @pytest.mark.asyncio
    async def test_get_traces_by_job_id_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test get_traces_by_job_id handles SQLAlchemy errors."""
        job_id = "job-123"
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.get_traces_by_job_id(
                db=None, job_id=job_id, limit=100, offset=0
            )
    
    @pytest.mark.asyncio
    async def test_get_traces_by_job_id_general_error(self, mock_execution_trace_repository):
        """Test get_traces_by_job_id handles general errors."""
        job_id = "job-123"
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.get_traces_by_job_id(
                db=None, job_id=job_id, limit=100, offset=0
            )
    
    @pytest.mark.asyncio
    async def test_get_all_traces_success(self, mock_execution_trace_repository, mock_traces):
        """Test successful retrieval of all traces."""
        total_count = 3
        
        mock_execution_trace_repository.get_all_traces.return_value = (mock_traces, total_count)
        
        result = await ExecutionTraceService.get_all_traces(limit=100, offset=0)
        
        assert isinstance(result, ExecutionTraceList)
        assert len(result.traces) == 3
        assert result.total == total_count
        assert result.limit == 100
        assert result.offset == 0
        assert all(isinstance(trace, ExecutionTraceItem) for trace in result.traces)
        
        mock_execution_trace_repository.get_all_traces.assert_called_once_with(100, 0)
    
    @pytest.mark.asyncio
    async def test_get_all_traces_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test get_all_traces handles SQLAlchemy errors."""
        mock_execution_trace_repository.get_all_traces.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.get_all_traces(limit=100, offset=0)
    
    @pytest.mark.asyncio
    async def test_get_all_traces_general_error(self, mock_execution_trace_repository):
        """Test get_all_traces handles general errors."""
        mock_execution_trace_repository.get_all_traces.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.get_all_traces(limit=100, offset=0)
    
    @pytest.mark.asyncio
    async def test_get_trace_by_id_success(self, mock_execution_trace_repository, mock_trace):
        """Test successful retrieval of trace by ID."""
        trace_id = 1
        
        mock_execution_trace_repository.get_by_id.return_value = mock_trace
        
        result = await ExecutionTraceService.get_trace_by_id(trace_id)
        
        assert isinstance(result, ExecutionTraceItem)
        assert result.id == trace_id
        
        mock_execution_trace_repository.get_by_id.assert_called_once_with(trace_id)
    
    @pytest.mark.asyncio
    async def test_get_trace_by_id_not_found(self, mock_execution_trace_repository):
        """Test get_trace_by_id when trace doesn't exist."""
        trace_id = 999
        
        mock_execution_trace_repository.get_by_id.return_value = None
        
        result = await ExecutionTraceService.get_trace_by_id(trace_id)
        
        assert result is None
        mock_execution_trace_repository.get_by_id.assert_called_once_with(trace_id)
    
    @pytest.mark.asyncio
    async def test_get_trace_by_id_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test get_trace_by_id handles SQLAlchemy errors."""
        trace_id = 1
        
        mock_execution_trace_repository.get_by_id.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.get_trace_by_id(trace_id)
    
    @pytest.mark.asyncio
    async def test_get_trace_by_id_general_error(self, mock_execution_trace_repository):
        """Test get_trace_by_id handles general errors."""
        trace_id = 1
        
        mock_execution_trace_repository.get_by_id.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.get_trace_by_id(trace_id)
    
    @pytest.mark.asyncio
    async def test_create_trace_success(self, mock_execution_trace_repository, mock_trace):
        """Test successful trace creation."""
        trace_data = {
            "run_id": 1,
            "job_id": "job-123",
            "event_source": "test",
            "event_type": "test_event",
            "input_data": {"input": "test"},
            "output_data": {"output": "test"}
        }
        
        mock_execution_trace_repository.create.return_value = mock_trace
        
        result = await ExecutionTraceService.create_trace(trace_data)
        
        assert isinstance(result, ExecutionTraceItem)
        assert result.id == mock_trace.id
        
        mock_execution_trace_repository.create.assert_called_once_with(trace_data)
    
    @pytest.mark.asyncio
    async def test_create_trace_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test create_trace handles SQLAlchemy errors."""
        trace_data = {"run_id": 1, "job_id": "job-123"}
        
        mock_execution_trace_repository.create.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.create_trace(trace_data)
    
    @pytest.mark.asyncio
    async def test_create_trace_general_error(self, mock_execution_trace_repository):
        """Test create_trace handles general errors."""
        trace_data = {"run_id": 1, "job_id": "job-123"}
        
        mock_execution_trace_repository.create.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.create_trace(trace_data)
    
    @pytest.mark.asyncio
    async def test_delete_trace_success(self, mock_execution_trace_repository, mock_trace):
        """Test successful trace deletion."""
        trace_id = 1
        
        mock_execution_trace_repository.get_by_id.return_value = mock_trace
        mock_execution_trace_repository.delete_by_id.return_value = 1
        
        result = await ExecutionTraceService.delete_trace(trace_id)
        
        assert isinstance(result, DeleteTraceResponse)
        assert result.deleted_traces == 1
        assert f"Successfully deleted trace {trace_id}" in result.message
        
        mock_execution_trace_repository.get_by_id.assert_called_once_with(trace_id)
        mock_execution_trace_repository.delete_by_id.assert_called_once_with(trace_id)
    
    @pytest.mark.asyncio
    async def test_delete_trace_not_found(self, mock_execution_trace_repository):
        """Test delete_trace when trace doesn't exist."""
        trace_id = 999
        
        mock_execution_trace_repository.get_by_id.return_value = None
        
        result = await ExecutionTraceService.delete_trace(trace_id)
        
        assert result is None
        mock_execution_trace_repository.get_by_id.assert_called_once_with(trace_id)
        mock_execution_trace_repository.delete_by_id.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_trace_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test delete_trace handles SQLAlchemy errors."""
        trace_id = 1
        
        mock_execution_trace_repository.get_by_id.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.delete_trace(trace_id)
    
    @pytest.mark.asyncio
    async def test_delete_trace_general_error(self, mock_execution_trace_repository):
        """Test delete_trace handles general errors."""
        trace_id = 1
        
        mock_execution_trace_repository.get_by_id.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.delete_trace(trace_id)
    
    @pytest.mark.asyncio
    async def test_delete_traces_by_run_id_success(self, mock_execution_trace_repository):
        """Test successful deletion of traces by run_id."""
        run_id = 1
        deleted_count = 3
        
        mock_execution_trace_repository.delete_by_run_id.return_value = deleted_count
        
        result = await ExecutionTraceService.delete_traces_by_run_id(run_id)
        
        assert isinstance(result, DeleteTraceResponse)
        assert result.deleted_traces == deleted_count
        assert f"Successfully deleted {deleted_count} traces for execution {run_id}" in result.message
        
        mock_execution_trace_repository.delete_by_run_id.assert_called_once_with(run_id)
    
    @pytest.mark.asyncio
    async def test_delete_traces_by_run_id_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test delete_traces_by_run_id handles SQLAlchemy errors."""
        run_id = 1
        
        mock_execution_trace_repository.delete_by_run_id.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.delete_traces_by_run_id(run_id)
    
    @pytest.mark.asyncio
    async def test_delete_traces_by_run_id_general_error(self, mock_execution_trace_repository):
        """Test delete_traces_by_run_id handles general errors."""
        run_id = 1
        
        mock_execution_trace_repository.delete_by_run_id.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.delete_traces_by_run_id(run_id)
    
    @pytest.mark.asyncio
    async def test_delete_traces_by_job_id_success(self, mock_execution_trace_repository):
        """Test successful deletion of traces by job_id."""
        job_id = "job-123"
        deleted_count = 2
        
        mock_execution_trace_repository.delete_by_job_id.return_value = deleted_count
        
        result = await ExecutionTraceService.delete_traces_by_job_id(job_id)
        
        assert isinstance(result, DeleteTraceResponse)
        assert result.deleted_traces == deleted_count
        assert f"Successfully deleted {deleted_count} traces for job {job_id}" in result.message
        
        mock_execution_trace_repository.delete_by_job_id.assert_called_once_with(job_id)
    
    @pytest.mark.asyncio
    async def test_delete_traces_by_job_id_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test delete_traces_by_job_id handles SQLAlchemy errors."""
        job_id = "job-123"
        
        mock_execution_trace_repository.delete_by_job_id.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.delete_traces_by_job_id(job_id)
    
    @pytest.mark.asyncio
    async def test_delete_traces_by_job_id_general_error(self, mock_execution_trace_repository):
        """Test delete_traces_by_job_id handles general errors."""
        job_id = "job-123"
        
        mock_execution_trace_repository.delete_by_job_id.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.delete_traces_by_job_id(job_id)
    
    @pytest.mark.asyncio
    async def test_delete_all_traces_success(self, mock_execution_trace_repository):
        """Test successful deletion of all traces."""
        deleted_count = 10
        
        mock_execution_trace_repository.delete_all.return_value = deleted_count
        
        result = await ExecutionTraceService.delete_all_traces()
        
        assert isinstance(result, DeleteTraceResponse)
        assert result.deleted_traces == deleted_count
        assert f"Successfully deleted all traces ({deleted_count} total)" in result.message
        
        mock_execution_trace_repository.delete_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_traces_sqlalchemy_error(self, mock_execution_trace_repository):
        """Test delete_all_traces handles SQLAlchemy errors."""
        mock_execution_trace_repository.delete_all.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await ExecutionTraceService.delete_all_traces()
    
    @pytest.mark.asyncio
    async def test_delete_all_traces_general_error(self, mock_execution_trace_repository):
        """Test delete_all_traces handles general errors."""
        mock_execution_trace_repository.delete_all.side_effect = Exception("General error")
        
        with pytest.raises(Exception):
            await ExecutionTraceService.delete_all_traces()
    
    @pytest.mark.asyncio
    async def test_get_traces_by_run_id_custom_pagination(self, mock_execution_trace_repository, mock_traces):
        """Test get_traces_by_run_id with custom pagination parameters."""
        run_id = 1
        job_id = "job-123"
        limit = 50
        offset = 10
        
        mock_execution_trace_repository.get_execution_job_id_by_run_id.return_value = job_id
        mock_execution_trace_repository.get_by_run_id.return_value = mock_traces[:1]
        
        result = await ExecutionTraceService.get_traces_by_run_id(
            db=None, run_id=run_id, limit=limit, offset=offset
        )
        
        assert result is not None
        mock_execution_trace_repository.get_by_run_id.assert_called_once_with(run_id, limit, offset)
    
    @pytest.mark.asyncio
    async def test_get_traces_by_job_id_custom_pagination(self, mock_execution_trace_repository, mock_traces):
        """Test get_traces_by_job_id with custom pagination parameters."""
        job_id = "job-123"
        run_id = 1
        limit = 25
        offset = 5
        
        mock_execution_trace_repository.get_execution_run_id_by_job_id.return_value = run_id
        mock_execution_trace_repository.get_by_job_id.return_value = mock_traces[:1]
        
        result = await ExecutionTraceService.get_traces_by_job_id(
            db=None, job_id=job_id, limit=limit, offset=offset
        )
        
        assert result is not None
        mock_execution_trace_repository.get_by_job_id.assert_called_once_with(job_id, limit, offset)
    
    @pytest.mark.asyncio
    async def test_get_all_traces_custom_pagination(self, mock_execution_trace_repository, mock_traces):
        """Test get_all_traces with custom pagination parameters."""
        limit = 20
        offset = 40
        total_count = 100
        
        mock_execution_trace_repository.get_all_traces.return_value = (mock_traces, total_count)
        
        result = await ExecutionTraceService.get_all_traces(limit=limit, offset=offset)
        
        assert result.limit == limit
        assert result.offset == offset
        assert result.total == total_count
        mock_execution_trace_repository.get_all_traces.assert_called_once_with(limit, offset)
    
    def test_execution_trace_service_class_methods(self):
        """Test that all ExecutionTraceService methods exist and are callable."""
        methods = [
            'get_traces_by_run_id',
            'get_traces_by_job_id', 
            'get_all_traces',
            'get_trace_by_id',
            'create_trace',
            'delete_trace',
            'delete_traces_by_run_id',
            'delete_traces_by_job_id',
            'delete_all_traces'
        ]
        
        for method_name in methods:
            assert hasattr(ExecutionTraceService, method_name), f"ExecutionTraceService should have {method_name} method"
            method = getattr(ExecutionTraceService, method_name)
            assert callable(method), f"{method_name} should be callable"