import pytest
import uuid
from unittest.mock import Mock, patch, AsyncMock
from sqlalchemy.orm import Session
from fastapi import HTTPException

from src.engines.crewai.crewai_flow_service import CrewAIFlowService


class TestCrewAIFlowService:
    """Test cases for CrewAIFlowService - targeting 100% coverage."""

    @pytest.fixture
    def mock_session(self):
        """Mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service_with_session(self, mock_session):
        """Create CrewAIFlowService with mocked session."""
        return CrewAIFlowService(mock_session)

    @pytest.fixture
    def service_without_session(self):
        """Create CrewAIFlowService without session."""
        return CrewAIFlowService()

    def test_init_with_session(self, mock_session):
        """Test CrewAIFlowService initialization with session."""
        service = CrewAIFlowService(mock_session)
        assert service.session == mock_session

    def test_init_without_session(self):
        """Test CrewAIFlowService initialization without session."""
        service = CrewAIFlowService()
        assert service.session is None

    def test_init_with_none_session(self):
        """Test CrewAIFlowService initialization with None session."""
        service = CrewAIFlowService(None)
        assert service.session is None

    @patch('src.engines.crewai.crewai_flow_service.FlowRunnerService')
    def test_get_flow_runner_with_session(self, mock_flow_runner_class, service_with_session, mock_session):
        """Test _get_flow_runner when service has a session."""
        mock_flow_runner = Mock()
        mock_flow_runner_class.return_value = mock_flow_runner
        
        result = service_with_session._get_flow_runner()
        
        mock_flow_runner_class.assert_called_once_with(mock_session)
        assert result == mock_flow_runner

    @patch('src.engines.crewai.crewai_flow_service.SessionLocal')
    @patch('src.engines.crewai.crewai_flow_service.FlowRunnerService')
    def test_get_flow_runner_without_session(self, mock_flow_runner_class, mock_session_local, service_without_session):
        """Test _get_flow_runner when service has no session."""
        mock_new_session = Mock()
        mock_session_local.return_value = mock_new_session
        mock_flow_runner = Mock()
        mock_flow_runner_class.return_value = mock_flow_runner
        
        result = service_without_session._get_flow_runner()
        
        mock_session_local.assert_called_once()
        mock_flow_runner_class.assert_called_once_with(mock_new_session)
        assert result == mock_flow_runner

    @pytest.mark.asyncio
    async def test_run_flow_with_all_parameters(self, service_without_session):
        """Test run_flow with all parameters provided."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        config = {"key": "value"}
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {
            "success": True,
            "execution_id": 1,
            "job_id": job_id,
            "flow_id": flow_id,
            "status": "PENDING"
        }
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service_without_session.run_flow(flow_id, job_id, config)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id=job_id,
            config=config
        )
        assert result["success"] is True
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_run_flow_with_string_flow_id(self, service_without_session):
        """Test run_flow with string flow_id."""
        flow_id = "550e8400-e29b-41d4-a716-446655440000"
        job_id = "test-job-123"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service_without_session.run_flow(flow_id, job_id)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id=job_id,
            config={}
        )
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_run_flow_without_job_id(self, service_without_session):
        """Test run_flow without job_id - should generate one."""
        flow_id = uuid.uuid4()
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with patch('src.engines.crewai.crewai_flow_service.uuid.uuid4') as mock_uuid:
                mock_uuid.return_value = "generated-uuid"
                await service_without_session.run_flow(flow_id)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id="generated-uuid",
            config={}
        )

    @pytest.mark.asyncio
    async def test_run_flow_without_config(self, service_without_session):
        """Test run_flow without config - should use empty dict."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            await service_without_session.run_flow(flow_id, job_id)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id=job_id,
            config={}
        )

    @pytest.mark.asyncio
    async def test_run_flow_with_none_config(self, service_without_session):
        """Test run_flow with None config - should use empty dict."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            await service_without_session.run_flow(flow_id, job_id, None)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id=job_id,
            config={}
        )

    @pytest.mark.asyncio
    async def test_run_flow_with_empty_job_id(self, service_without_session):
        """Test run_flow with empty string job_id - should generate one."""
        flow_id = uuid.uuid4()
        job_id = ""
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with patch('src.engines.crewai.crewai_flow_service.uuid.uuid4') as mock_uuid:
                mock_uuid.return_value = "generated-uuid"
                await service_without_session.run_flow(flow_id, job_id)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id="generated-uuid",
            config={}
        )

    @pytest.mark.asyncio
    async def test_run_flow_flow_runner_exception(self, service_without_session):
        """Test run_flow when flow runner raises exception."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.side_effect = Exception("Flow runner error")
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
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
        expected_result = {
            "success": True,
            "execution": {"id": 1, "status": "COMPLETED"}
        }
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_execution.return_value = expected_result
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service_without_session.get_flow_execution(execution_id)
        
        mock_flow_runner.get_flow_execution.assert_called_once_with(execution_id)
        assert result == expected_result

    @pytest.mark.asyncio
    async def test_get_flow_execution_flow_runner_exception(self, service_without_session):
        """Test get_flow_execution when flow runner raises exception."""
        execution_id = 1
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_execution.side_effect = Exception("Database error")
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
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
        expected_result = {
            "success": True,
            "flow_id": flow_id,
            "executions": []
        }
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_executions_by_flow.return_value = expected_result
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service_without_session.get_flow_executions_by_flow(flow_id)
        
        mock_flow_runner.get_flow_executions_by_flow.assert_called_once_with(flow_id)
        assert result == expected_result

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_with_string_uuid(self, service_without_session):
        """Test get_flow_executions_by_flow with valid string UUID."""
        flow_id_str = "550e8400-e29b-41d4-a716-446655440000"
        flow_id_uuid = uuid.UUID(flow_id_str)
        expected_result = {"success": True, "flow_id": flow_id_uuid}
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_executions_by_flow.return_value = expected_result
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service_without_session.get_flow_executions_by_flow(flow_id_str)
        
        mock_flow_runner.get_flow_executions_by_flow.assert_called_once_with(flow_id_uuid)
        assert result == expected_result

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
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_executions_by_flow.side_effect = Exception("Database error")
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with pytest.raises(HTTPException) as exc_info:
                await service_without_session.get_flow_executions_by_flow(flow_id)
        
        assert exc_info.value.status_code == 500
        assert "Error getting flow executions: Database error" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_http_exception_reraise(self, service_without_session):
        """Test get_flow_executions_by_flow re-raises HTTPException."""
        flow_id = uuid.uuid4()
        
        original_http_exception = HTTPException(status_code=404, detail="Not found")
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_executions_by_flow.side_effect = original_http_exception
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
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

    @pytest.mark.asyncio
    @patch('src.engines.crewai.crewai_flow_service.logger')
    async def test_run_flow_logging(self, mock_logger, service_without_session):
        """Test run_flow logging behavior."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            await service_without_session.run_flow(flow_id, job_id)
        
        # Verify logging calls
        mock_logger.info.assert_any_call(f"CrewAIFlowService.run_flow called with flow_id={flow_id}, job_id={job_id}")
        mock_logger.info.assert_any_call(f"Flow execution started successfully: {{'success': True}}")

    @pytest.mark.asyncio
    @patch('src.engines.crewai.crewai_flow_service.logger')
    async def test_run_flow_logging_generated_job_id(self, mock_logger, service_without_session):
        """Test run_flow logging when job_id is generated."""
        flow_id = uuid.uuid4()
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with patch('src.engines.crewai.crewai_flow_service.uuid.uuid4') as mock_uuid:
                mock_uuid.return_value = "generated-uuid"
                await service_without_session.run_flow(flow_id)
        
        # Verify logging for generated job_id
        mock_logger.info.assert_any_call("Generated job_id: generated-uuid")

    @pytest.mark.asyncio
    @patch('src.engines.crewai.crewai_flow_service.logger')
    async def test_run_flow_exception_logging(self, mock_logger, service_without_session):
        """Test run_flow exception logging."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        error_message = "Test error"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.side_effect = Exception(error_message)
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with pytest.raises(HTTPException):
                await service_without_session.run_flow(flow_id, job_id)
        
        # Verify error logging
        mock_logger.error.assert_called_once_with(
            f"Error executing flow: {error_message}",
            exc_info=True
        )

    @pytest.mark.asyncio
    @patch('src.engines.crewai.crewai_flow_service.logger')
    async def test_get_flow_execution_exception_logging(self, mock_logger, service_without_session):
        """Test get_flow_execution exception logging."""
        execution_id = 1
        error_message = "Database error"
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_execution.side_effect = Exception(error_message)
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with pytest.raises(HTTPException):
                await service_without_session.get_flow_execution(execution_id)
        
        # Verify error logging
        mock_logger.error.assert_called_once_with(
            f"Error getting flow execution: {error_message}",
            exc_info=True
        )

    @pytest.mark.asyncio
    @patch('src.engines.crewai.crewai_flow_service.logger')
    async def test_get_flow_executions_by_flow_exception_logging(self, mock_logger, service_without_session):
        """Test get_flow_executions_by_flow exception logging."""
        flow_id = uuid.uuid4()
        error_message = "Database error"
        
        mock_flow_runner = Mock()
        mock_flow_runner.get_flow_executions_by_flow.side_effect = Exception(error_message)
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with pytest.raises(HTTPException):
                await service_without_session.get_flow_executions_by_flow(flow_id)
        
        # Verify error logging
        mock_logger.error.assert_called_once_with(
            f"Error getting flow executions: {error_message}",
            exc_info=True
        )

    @pytest.mark.asyncio
    async def test_run_flow_with_none_flow_id(self, service_without_session):
        """Test run_flow with None flow_id."""
        job_id = "test-job-123"
        
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            await service_without_session.run_flow(None, job_id)
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=None,
            job_id=job_id,
            config={}
        )

    @pytest.mark.asyncio 
    async def test_run_flow_with_all_none_parameters(self, service_without_session):
        """Test run_flow with all None parameters."""
        mock_flow_runner = AsyncMock()
        mock_flow_runner.run_flow.return_value = {"success": True}
        
        with patch.object(service_without_session, '_get_flow_runner', return_value=mock_flow_runner):
            with patch('src.engines.crewai.crewai_flow_service.uuid.uuid4') as mock_uuid:
                mock_uuid.return_value = "generated-uuid"
                await service_without_session.run_flow()
        
        mock_flow_runner.run_flow.assert_called_once_with(
            flow_id=None,
            job_id="generated-uuid",
            config={}
        )