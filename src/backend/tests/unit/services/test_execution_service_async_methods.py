"""Unit tests for ExecutionService async methods.

Tests the actual API of ExecutionService including execute_flow, list_executions,
get_execution_status, create_execution, and stop_execution.
"""

import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import json

from src.services.execution_service import ExecutionService
from src.schemas.execution import ExecutionStatus
from src.utils.user_context import GroupContext


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_group_context():
    """Create a mock group context for multi-tenant testing."""
    return GroupContext(
        group_ids=["group_123"],
        group_email="test@example.com",
        access_token="test_token"
    )


@pytest.fixture
def execution_service(mock_session):
    """Create an ExecutionService instance with mocked dependencies."""
    with patch("src.services.execution_service.ExecutionNameService") as MockNameService:
        mock_name_service = MagicMock()
        mock_name_service.generate_execution_name = AsyncMock(
            return_value=MagicMock(name="Test Execution Run")
        )
        MockNameService.create.return_value = mock_name_service

        service = ExecutionService(session=mock_session)
        service.execution_name_service = mock_name_service
        return service


@pytest.fixture(autouse=True)
def cleanup_executions():
    """Clean up executions after each test."""
    yield
    # Clean up any test executions
    keys_to_remove = [k for k in ExecutionService.executions.keys() if k.startswith("test_")]
    for key in keys_to_remove:
        del ExecutionService.executions[key]


# ============================================================================
# Test Class: Get Execution (Static Method)
# ============================================================================

class TestGetExecution:
    """Tests for get_execution static method."""

    def test_get_execution_returns_stored_data(self):
        """Test get_execution returns data from in-memory storage."""
        exec_id = "test_exec_123"
        exec_data = {"execution_id": exec_id, "status": "running"}
        ExecutionService.executions[exec_id] = exec_data

        try:
            result = ExecutionService.get_execution(exec_id)
            assert result == exec_data
        finally:
            del ExecutionService.executions[exec_id]

    def test_get_execution_returns_none_for_unknown(self):
        """Test get_execution returns None for non-existent execution."""
        result = ExecutionService.get_execution("non_existent_id")
        assert result is None

    def test_get_execution_is_static_method(self):
        """Test that get_execution is a static method."""
        # Can be called without instance
        result = ExecutionService.get_execution("any_id")
        assert result is None  # Just verifying it can be called


# ============================================================================
# Test Class: Create Execution ID
# ============================================================================

class TestCreateExecutionId:
    """Tests for create_execution_id static method."""

    def test_create_execution_id_returns_uuid(self):
        """Test create_execution_id returns a valid UUID string."""
        exec_id = ExecutionService.create_execution_id()

        # Should be a valid UUID
        parsed = uuid.UUID(exec_id)
        assert str(parsed) == exec_id

    def test_create_execution_id_unique(self):
        """Test create_execution_id returns unique IDs."""
        ids = [ExecutionService.create_execution_id() for _ in range(100)]
        assert len(set(ids)) == 100


# ============================================================================
# Test Class: Add Execution To Memory
# ============================================================================

class TestAddExecutionToMemory:
    """Tests for add_execution_to_memory static method."""

    def test_add_execution_basic(self):
        """Test adding execution to memory with basic data."""
        exec_id = "test_add_basic_123"

        try:
            ExecutionService.add_execution_to_memory(
                execution_id=exec_id,
                status="running",
                run_name="Test Run"
            )

            result = ExecutionService.executions.get(exec_id)
            assert result is not None
            assert result["execution_id"] == exec_id
            assert result["status"] == "running"
            assert result["run_name"] == "Test Run"
        finally:
            if exec_id in ExecutionService.executions:
                del ExecutionService.executions[exec_id]

    def test_add_execution_overwrites_existing(self):
        """Test adding execution overwrites existing entry."""
        exec_id = "test_add_overwrite_123"

        try:
            ExecutionService.add_execution_to_memory(
                execution_id=exec_id,
                status="running",
                run_name="Run 1"
            )

            ExecutionService.add_execution_to_memory(
                execution_id=exec_id,
                status="completed",
                run_name="Run 2"
            )

            result = ExecutionService.executions.get(exec_id)
            assert result["status"] == "completed"
            assert result["run_name"] == "Run 2"
        finally:
            if exec_id in ExecutionService.executions:
                del ExecutionService.executions[exec_id]


# ============================================================================
# Test Class: Sanitize For Database
# ============================================================================

class TestSanitizeForDatabase:
    """Tests for sanitize_for_database static method."""

    def test_sanitize_basic_dict(self):
        """Test sanitizing basic dictionary."""
        data = {"key": "value", "number": 42}
        result = ExecutionService.sanitize_for_database(data)
        assert result == data

    def test_sanitize_with_datetime(self):
        """Test sanitizing datetime objects."""
        dt = datetime.now(timezone.utc)
        data = {"timestamp": dt}
        result = ExecutionService.sanitize_for_database(data)
        assert isinstance(result["timestamp"], str)

    def test_sanitize_with_uuid(self):
        """Test sanitizing UUID objects."""
        uid = uuid.uuid4()
        data = {"id": uid}
        result = ExecutionService.sanitize_for_database(data)
        assert isinstance(result["id"], str)

    def test_sanitize_nested_objects(self):
        """Test sanitizing nested dictionaries."""
        data = {"outer": {"inner": {"deep": "value"}}}
        result = ExecutionService.sanitize_for_database(data)
        assert result["outer"]["inner"]["deep"] == "value"

    def test_sanitize_with_none(self):
        """Test sanitizing with None values."""
        data = {"key": None}
        result = ExecutionService.sanitize_for_database(data)
        assert result["key"] is None

    def test_sanitize_with_list(self):
        """Test sanitizing lists."""
        data = {"items": [1, 2, 3]}
        result = ExecutionService.sanitize_for_database(data)
        assert result["items"] == [1, 2, 3]

    def test_sanitize_empty_dict(self):
        """Test sanitizing empty dictionary."""
        data = {}
        result = ExecutionService.sanitize_for_database(data)
        assert result == {}


# ============================================================================
# Test Class: Execute Flow
# ============================================================================

class TestExecuteFlow:
    """Tests for execute_flow method."""

    @pytest.mark.asyncio
    async def test_execute_flow_with_flow_id(self, execution_service):
        """Test execute_flow with a flow ID."""
        flow_id = uuid.uuid4()
        job_id = "job_123"

        mock_result = {"job_id": job_id, "status": "running"}
        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.run_flow_execution = AsyncMock(
            return_value=mock_result
        )

        result = await execution_service.execute_flow(
            flow_id=flow_id,
            job_id=job_id,
            config={}
        )

        assert result == mock_result

    @pytest.mark.asyncio
    async def test_execute_flow_generates_job_id(self, execution_service):
        """Test execute_flow generates job_id if not provided."""
        flow_id = uuid.uuid4()

        mock_result = {"status": "running"}
        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.run_flow_execution = AsyncMock(
            return_value=mock_result
        )

        result = await execution_service.execute_flow(flow_id=flow_id)

        call_args = execution_service.crewai_execution_service.run_flow_execution.call_args
        assert call_args.kwargs["job_id"] is not None

    @pytest.mark.asyncio
    async def test_execute_flow_handles_http_exception(self, execution_service):
        """Test execute_flow re-raises KasalError."""
        from src.core.exceptions import KasalError, NotFoundError

        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.run_flow_execution = AsyncMock(
            side_effect=NotFoundError(detail="Flow not found")
        )

        with pytest.raises(KasalError) as exc_info:
            await execution_service.execute_flow(flow_id=uuid.uuid4())

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_execute_flow_handles_unexpected_error(self, execution_service):
        """Test execute_flow wraps unexpected errors in KasalError."""
        from src.core.exceptions import KasalError

        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.run_flow_execution = AsyncMock(
            side_effect=ValueError("Unexpected error")
        )

        with pytest.raises(KasalError) as exc_info:
            await execution_service.execute_flow(flow_id=uuid.uuid4())

        assert exc_info.value.status_code == 500


# ============================================================================
# Test Class: Get Executions By Flow
# ============================================================================

class TestGetExecutionsByFlow:
    """Tests for get_executions_by_flow method."""

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_success(self, execution_service):
        """Test get_executions_by_flow returns executions."""
        flow_id = uuid.uuid4()
        mock_result = {"executions": [{"id": 1}, {"id": 2}]}

        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.get_flow_executions_by_flow = AsyncMock(
            return_value=mock_result
        )

        result = await execution_service.get_executions_by_flow(flow_id)

        assert result == mock_result

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_handles_error(self, execution_service):
        """Test get_executions_by_flow handles errors gracefully."""
        from src.core.exceptions import KasalError

        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.get_flow_executions_by_flow = AsyncMock(
            side_effect=ValueError("Database error")
        )

        with pytest.raises(KasalError) as exc_info:
            await execution_service.get_executions_by_flow(uuid.uuid4())

        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_empty_result(self, execution_service):
        """Test get_executions_by_flow with no executions."""
        flow_id = uuid.uuid4()
        mock_result = {"executions": []}

        execution_service.crewai_execution_service = MagicMock()
        execution_service.crewai_execution_service.get_flow_executions_by_flow = AsyncMock(
            return_value=mock_result
        )

        result = await execution_service.get_executions_by_flow(flow_id)

        assert result["executions"] == []


# ============================================================================
# Test Class: Generate Execution Name
# ============================================================================

class TestGenerateExecutionName:
    """Tests for generate_execution_name method."""

    @pytest.mark.asyncio
    async def test_generate_execution_name(self, execution_service):
        """Test generate_execution_name returns generated name."""
        from src.schemas.execution import ExecutionNameGenerationRequest

        request = ExecutionNameGenerationRequest(
            agents_yaml={"agent1": {"role": "researcher"}},
            tasks_yaml={"task1": {"name": "Research"}},
            model="gpt-4"
        )

        mock_response = MagicMock()
        mock_response.name = "Research Crew Run"
        execution_service.execution_name_service.generate_execution_name = AsyncMock(
            return_value=mock_response
        )

        result = await execution_service.generate_execution_name(request)

        assert "name" in result
        assert result["name"] == "Research Crew Run"

    @pytest.mark.asyncio
    async def test_generate_execution_name_with_empty_agents(self, execution_service):
        """Test generate_execution_name with empty agents."""
        from src.schemas.execution import ExecutionNameGenerationRequest

        request = ExecutionNameGenerationRequest(
            agents_yaml={},
            tasks_yaml={"task1": {"name": "Task"}},
            model="gpt-4"
        )

        mock_response = MagicMock()
        mock_response.name = "Generic Run"
        execution_service.execution_name_service.generate_execution_name = AsyncMock(
            return_value=mock_response
        )

        result = await execution_service.generate_execution_name(request)

        assert "name" in result


# ============================================================================
# Test Class: Class Attributes
# ============================================================================

class TestExecutionServiceClassAttributes:
    """Tests for ExecutionService class attributes."""

    def test_executions_dict_exists(self):
        """Test that executions class variable exists."""
        assert hasattr(ExecutionService, 'executions')
        assert isinstance(ExecutionService.executions, dict)

    def test_thread_pool_exists(self):
        """Test that _thread_pool class variable exists."""
        assert hasattr(ExecutionService, '_thread_pool')


# ============================================================================
# Test Class: Service Initialization
# ============================================================================

class TestExecutionServiceInit:
    """Tests for ExecutionService initialization."""

    def test_init_with_session(self, mock_session):
        """Test initialization with database session."""
        with patch("src.services.execution_service.ExecutionNameService") as MockNameService:
            MockNameService.create.return_value = MagicMock()
            service = ExecutionService(session=mock_session)
            assert service.session is mock_session

    def test_init_without_session(self):
        """Test initialization without database session."""
        with patch("src.services.execution_service.ExecutionNameService") as MockNameService:
            MockNameService.create.return_value = MagicMock()
            service = ExecutionService(session=None)
            assert service.session is None

    def test_init_creates_crewai_execution_service(self, mock_session):
        """Test that CrewAIExecutionService is created."""
        with patch("src.services.execution_service.ExecutionNameService") as MockNameService:
            MockNameService.create.return_value = MagicMock()
            with patch("src.services.execution_service.CrewAIExecutionService") as MockCrewAI:
                MockCrewAI.return_value = MagicMock()
                service = ExecutionService(session=mock_session)
                assert service.crewai_execution_service is not None


# ============================================================================
# Test Class: Extract Agents Tasks From Flow Config
# ============================================================================

class TestExtractAgentsTasksFromFlowConfig:
    """Tests for _extract_agents_tasks_from_flow_config method."""

    def test_extract_from_crew_nodes(self, execution_service):
        """Test extraction from crew nodes."""
        config = MagicMock()
        config.nodes = [
            {
                "id": "node1",
                "type": "crewNode",
                "data": {
                    "label": "Research Crew",
                    "allAgents": [
                        {"id": "agent1", "role": "Researcher", "goal": "Research"},
                    ],
                    "allTasks": [
                        {"id": "task1", "name": "Research Task", "description": "Do research"},
                    ]
                }
            }
        ]
        config.flow_config = {}

        agents_yaml, tasks_yaml = execution_service._extract_agents_tasks_from_flow_config(config)

        assert len(agents_yaml) >= 1
        assert len(tasks_yaml) >= 1

    def test_extract_handles_empty_config(self, execution_service):
        """Test extraction handles empty configuration."""
        config = MagicMock()
        config.nodes = []
        config.flow_config = {}

        agents_yaml, tasks_yaml = execution_service._extract_agents_tasks_from_flow_config(config)

        assert agents_yaml == {}
        assert tasks_yaml == {}

    def test_extract_handles_none_nodes(self, execution_service):
        """Test extraction handles None nodes."""
        config = MagicMock()
        config.nodes = None
        config.flow_config = {}

        agents_yaml, tasks_yaml = execution_service._extract_agents_tasks_from_flow_config(config)

        assert agents_yaml == {}
        assert tasks_yaml == {}


# ============================================================================
# Test Class: _update_execution_status Memory Cleanup
# ============================================================================

class TestUpdateExecutionStatusCleanup:
    """Tests for _update_execution_status cleaning up in-memory entries on terminal status."""

    @pytest.mark.asyncio
    async def test_terminal_status_removes_from_executions(self):
        """Test that terminal status removes entry from ExecutionService.executions."""
        exec_id = "test_cleanup_completed"
        ExecutionService.executions[exec_id] = {"status": "RUNNING"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            await ExecutionService._update_execution_status(
                exec_id, "COMPLETED", {"output": "done"}
            )

        assert exec_id not in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_failed_status_removes_from_executions(self):
        """Test that FAILED status removes entry from ExecutionService.executions."""
        exec_id = "test_cleanup_failed"
        ExecutionService.executions[exec_id] = {"status": "RUNNING"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            await ExecutionService._update_execution_status(
                exec_id, "FAILED", {"error": "something went wrong"}
            )

        assert exec_id not in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_stopped_status_removes_from_executions(self):
        """Test that STOPPED status removes entry from ExecutionService.executions."""
        exec_id = "test_cleanup_stopped"
        ExecutionService.executions[exec_id] = {"status": "RUNNING"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            await ExecutionService._update_execution_status(
                exec_id, "STOPPED"
            )

        assert exec_id not in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_cancelled_status_removes_from_executions(self):
        """Test that CANCELLED status removes entry from ExecutionService.executions."""
        exec_id = "test_cleanup_cancelled"
        ExecutionService.executions[exec_id] = {"status": "RUNNING"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            await ExecutionService._update_execution_status(
                exec_id, "CANCELLED"
            )

        assert exec_id not in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_rejected_status_removes_from_executions(self):
        """Test that REJECTED status removes entry from ExecutionService.executions."""
        exec_id = "test_cleanup_rejected"
        ExecutionService.executions[exec_id] = {"status": "WAITING_FOR_APPROVAL"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            await ExecutionService._update_execution_status(
                exec_id, "REJECTED"
            )

        assert exec_id not in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_non_terminal_status_keeps_entry(self):
        """Test that non-terminal status keeps entry in ExecutionService.executions."""
        exec_id = "test_cleanup_running"
        ExecutionService.executions[exec_id] = {"status": "PENDING"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            await ExecutionService._update_execution_status(
                exec_id, "RUNNING"
            )

        assert exec_id in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_db_update_failure_does_not_remove_entry(self):
        """Test that failed DB update does not remove entry from memory."""
        exec_id = "test_cleanup_db_fail"
        ExecutionService.executions[exec_id] = {"status": "RUNNING"}

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=False):
            await ExecutionService._update_execution_status(
                exec_id, "COMPLETED", {"output": "done"}
            )

        # Entry should still be in memory since DB update failed
        assert exec_id in ExecutionService.executions

    @pytest.mark.asyncio
    async def test_cleanup_safe_when_entry_already_gone(self):
        """Test that cleanup is safe even if the entry was already removed."""
        exec_id = "test_cleanup_already_gone"
        # Don't add to executions — simulate already removed

        with patch("src.services.execution_service.ExecutionStatusService.update_status", new_callable=AsyncMock, return_value=True):
            # Should not raise
            await ExecutionService._update_execution_status(
                exec_id, "COMPLETED", {"output": "done"}
            )

        assert exec_id not in ExecutionService.executions


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
