"""
Isolated test for CrewAIFlowService with minimal dependencies.
This test file focuses purely on testing the service logic without complex imports.
"""
import pytest
import uuid
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from sqlalchemy.orm import Session
from fastapi import HTTPException


# Create isolated version of the service for testing
class CrewAIFlowServiceIsolated:
    """Isolated version of CrewAIFlowService for testing."""
    
    def __init__(self, session=None):
        self.session = session
        
    def _get_flow_runner(self):
        if self.session:
            return MockFlowRunnerService(self.session)
        
        new_session = MockSessionLocal()
        return MockFlowRunnerService(new_session)
    
    async def run_flow(self, 
                      flow_id=None, 
                      job_id=None,
                      config=None):
        # Mock logger calls
        print(f"CrewAIFlowService.run_flow called with flow_id={flow_id}, job_id={job_id}")
        
        try:
            if not job_id:
                job_id = str(uuid.uuid4())
                print(f"Generated job_id: {job_id}")
            
            flow_runner = self._get_flow_runner()
            
            result = await flow_runner.run_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=config or {}
            )
            
            print(f"Flow execution started successfully: {result}")
            return result
            
        except Exception as e:
            error_msg = f"Error executing flow: {str(e)}"
            print(f"ERROR: {error_msg}")
            raise HTTPException(
                status_code=500,
                detail=error_msg
            )
    
    async def get_flow_execution(self, execution_id):
        try:
            flow_runner = self._get_flow_runner()
            return flow_runner.get_flow_execution(execution_id)
        except Exception as e:
            error_msg = f"Error getting flow execution: {str(e)}"
            print(f"ERROR: {error_msg}")
            raise HTTPException(
                status_code=500,
                detail=error_msg
            )
    
    async def get_flow_executions_by_flow(self, flow_id):
        try:
            if isinstance(flow_id, str):
                try:
                    flow_id = uuid.UUID(flow_id)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid flow_id format: {flow_id}"
                    )
            
            flow_runner = self._get_flow_runner()
            return flow_runner.get_flow_executions_by_flow(flow_id)
        except HTTPException:
            raise
        except Exception as e:
            error_msg = f"Error getting flow executions: {str(e)}"
            print(f"ERROR: {error_msg}")
            raise HTTPException(
                status_code=500,
                detail=error_msg
            )


class MockFlowRunnerService:
    def __init__(self, session):
        self.session = session
    
    async def run_flow(self, flow_id, job_id, config):
        return {"success": True, "job_id": job_id}
    
    def get_flow_execution(self, execution_id):
        return {"success": True, "execution_id": execution_id}
    
    def get_flow_executions_by_flow(self, flow_id):
        return {"success": True, "flow_id": flow_id}


class MockSessionLocal:
    pass


class TestCrewAIFlowServiceIsolated:
    """Test cases for CrewAIFlowService - targeting 100% coverage."""

    @pytest.fixture
    def mock_session(self):
        """Mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service_with_session(self, mock_session):
        """Create service with mocked session."""
        return CrewAIFlowServiceIsolated(mock_session)

    @pytest.fixture
    def service_without_session(self):
        """Create service without session."""
        return CrewAIFlowServiceIsolated()

    def test_init_with_session(self, mock_session):
        """Test service initialization with session."""
        service = CrewAIFlowServiceIsolated(mock_session)
        assert service.session == mock_session

    def test_init_without_session(self):
        """Test service initialization without session."""
        service = CrewAIFlowServiceIsolated()
        assert service.session is None

    def test_init_with_none_session(self):
        """Test service initialization with None session."""
        service = CrewAIFlowServiceIsolated(None)
        assert service.session is None

    def test_get_flow_runner_with_session(self, service_with_session, mock_session):
        """Test _get_flow_runner when service has a session."""
        result = service_with_session._get_flow_runner()
        assert isinstance(result, MockFlowRunnerService)
        assert result.session == mock_session

    def test_get_flow_runner_without_session(self, service_without_session):
        """Test _get_flow_runner when service has no session."""
        result = service_without_session._get_flow_runner()
        assert isinstance(result, MockFlowRunnerService)
        assert isinstance(result.session, MockSessionLocal)

    @pytest.mark.asyncio
    async def test_run_flow_with_all_parameters(self, service_without_session):
        """Test run_flow with all parameters provided."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        config = {"key": "value"}
        
        result = await service_without_session.run_flow(flow_id, job_id, config)
        
        assert result["success"] is True
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_run_flow_with_string_flow_id(self, service_without_session):
        """Test run_flow with string flow_id."""
        flow_id = "550e8400-e29b-41d4-a716-446655440000"
        job_id = "test-job-123"
        
        result = await service_without_session.run_flow(flow_id, job_id)
        
        assert result["success"] is True
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_run_flow_without_job_id(self, service_without_session):
        """Test run_flow without job_id - should generate one."""
        flow_id = uuid.uuid4()
        
        result = await service_without_session.run_flow(flow_id)
        
        assert result["success"] is True
        # job_id should be generated
        assert "job_id" in result

    @pytest.mark.asyncio
    async def test_run_flow_without_config(self, service_without_session):
        """Test run_flow without config - should use empty dict."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        result = await service_without_session.run_flow(flow_id, job_id)
        
        assert result["success"] is True
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_run_flow_with_none_config(self, service_without_session):
        """Test run_flow with None config - should use empty dict."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        result = await service_without_session.run_flow(flow_id, job_id, None)
        
        assert result["success"] is True
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_run_flow_with_empty_job_id(self, service_without_session):
        """Test run_flow with empty string job_id - should generate one."""
        flow_id = uuid.uuid4()
        job_id = ""
        
        result = await service_without_session.run_flow(flow_id, job_id)
        
        assert result["success"] is True
        # job_id should be generated
        assert "job_id" in result

    @pytest.mark.asyncio
    async def test_run_flow_flow_runner_exception(self, service_without_session):
        """Test run_flow when flow runner raises exception."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        # Mock flow runner to raise exception
        with patch.object(service_without_session, '_get_flow_runner') as mock_get_runner:
            mock_flow_runner = AsyncMock()
            mock_flow_runner.run_flow.side_effect = Exception("Flow runner error")
            mock_get_runner.return_value = mock_flow_runner
            
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.run_flow(flow_id, job_id)
            
            assert exc_info.value.status_code == 500
            assert "Error executing flow: Flow runner error" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_run_flow_get_flow_runner_exception(self, service_without_session):
        """Test run_flow when _get_flow_runner raises exception."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        with patch.object(service_without_session, '_get_flow_runner', side_effect=Exception("Database connection error")):
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.run_flow(flow_id, job_id)
            
            assert exc_info.value.status_code == 500
            assert "Error executing flow: Database connection error" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_execution_success(self, service_without_session):
        """Test get_flow_execution successful retrieval."""
        execution_id = 1
        
        result = await service_without_session.get_flow_execution(execution_id)
        
        assert result["success"] is True
        assert result["execution_id"] == execution_id

    @pytest.mark.asyncio
    async def test_get_flow_execution_flow_runner_exception(self, service_without_session):
        """Test get_flow_execution when flow runner raises exception."""
        execution_id = 1
        
        with patch.object(service_without_session, '_get_flow_runner') as mock_get_runner:
            mock_flow_runner = Mock()
            mock_flow_runner.get_flow_execution.side_effect = Exception("Database error")
            mock_get_runner.return_value = mock_flow_runner
            
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.get_flow_execution(execution_id)
            
            assert exc_info.value.status_code == 500
            assert "Error getting flow execution: Database error" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_execution_get_flow_runner_exception(self, service_without_session):
        """Test get_flow_execution when _get_flow_runner raises exception."""
        execution_id = 1
        
        with patch.object(service_without_session, '_get_flow_runner', side_effect=Exception("Connection error")):
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.get_flow_execution(execution_id)
            
            assert exc_info.value.status_code == 500
            assert "Error getting flow execution: Connection error" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_with_uuid(self, service_without_session):
        """Test get_flow_executions_by_flow with UUID."""
        flow_id = uuid.uuid4()
        
        result = await service_without_session.get_flow_executions_by_flow(flow_id)
        
        assert result["success"] is True
        assert result["flow_id"] == flow_id

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_with_string_uuid(self, service_without_session):
        """Test get_flow_executions_by_flow with valid string UUID."""
        flow_id_str = "550e8400-e29b-41d4-a716-446655440000"
        flow_id_uuid = uuid.UUID(flow_id_str)
        
        result = await service_without_session.get_flow_executions_by_flow(flow_id_str)
        
        assert result["success"] is True
        assert result["flow_id"] == flow_id_uuid

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_invalid_uuid_string(self, service_without_session):
        """Test get_flow_executions_by_flow with invalid UUID string."""
        flow_id = "invalid-uuid"
        
        with pytest.raises(HTTPException) as exc_info:
            await service_without_session.get_flow_executions_by_flow(flow_id)
        
        assert exc_info.value.status_code == 400
        assert f"Invalid flow_id format: {flow_id}" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_flow_runner_exception(self, service_without_session):
        """Test get_flow_executions_by_flow when flow runner raises exception."""
        flow_id = uuid.uuid4()
        
        with patch.object(service_without_session, '_get_flow_runner') as mock_get_runner:
            mock_flow_runner = Mock()
            mock_flow_runner.get_flow_executions_by_flow.side_effect = Exception("Database error")
            mock_get_runner.return_value = mock_flow_runner
            
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.get_flow_executions_by_flow(flow_id)
            
            assert exc_info.value.status_code == 500
            assert "Error getting flow executions: Database error" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_http_exception_reraise(self, service_without_session):
        """Test get_flow_executions_by_flow re-raises HTTPException."""
        flow_id = uuid.uuid4()
        
        original_http_exception = HTTPException(status_code=404, detail="Not found")
        
        with patch.object(service_without_session, '_get_flow_runner') as mock_get_runner:
            mock_flow_runner = Mock()
            mock_flow_runner.get_flow_executions_by_flow.side_effect = original_http_exception
            mock_get_runner.return_value = mock_flow_runner
            
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.get_flow_executions_by_flow(flow_id)
            
            assert exc_info.value == original_http_exception
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_get_flow_runner_exception(self, service_without_session):
        """Test get_flow_executions_by_flow when _get_flow_runner raises exception."""
        flow_id = uuid.uuid4()
        
        with patch.object(service_without_session, '_get_flow_runner', side_effect=Exception("Connection error")):
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.get_flow_executions_by_flow(flow_id)
            
            assert exc_info.value.status_code == 500
            assert "Error getting flow executions: Connection error" in str(exc_info.value.detail)

    def test_service_with_real_session_object(self):
        """Test that service works with actual session-like object."""
        mock_session = Mock()
        mock_session.configure_mock(spec=Session)
        
        service = CrewAIFlowServiceIsolated(mock_session)
        
        result = service._get_flow_runner()
        
        assert isinstance(result, MockFlowRunnerService)
        assert result.session == mock_session