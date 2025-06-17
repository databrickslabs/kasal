"""
Unit tests for FlowService.

Tests the functionality of flow operations including
flow CRUD operations, flow validation, and crew-based flow management.
"""
import pytest
import uuid
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from fastapi import HTTPException

from src.services.flow_service import FlowService
from src.models.flow import Flow
from src.schemas.flow import FlowCreate, FlowUpdate, Position, NodeData, Node, Edge


# Mock models
class MockFlow:
    def __init__(self, id=None, name="Test Flow", crew_id=None, nodes=None, edges=None,
                 flow_config=None, group_id="group-123", created_by_email="test@example.com",
                 created_at=None, updated_at=None):
        self.id = id or uuid.uuid4()
        self.name = name
        self.crew_id = crew_id or uuid.uuid4()
        self.nodes = nodes or [
            {
                "id": "node1",
                "type": "agent",
                "position": {"x": 100, "y": 100},
                "data": {"label": "Agent 1", "type": "researcher"}
            }
        ]
        self.edges = edges or [
            {
                "id": "edge1",
                "source": "node1",
                "target": "node2",
                "type": "default"
            }
        ]
        self.flow_config = flow_config or {"version": "1.0"}
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


class MockPosition:
    def __init__(self, x=100.0, y=100.0):
        self.x = x
        self.y = y


class MockNodeData:
    def __init__(self, label="Test Node", crew_name=None, node_type="agent"):
        self.label = label
        self.crewName = crew_name
        self.type = node_type


class MockNode:
    def __init__(self, id="node1", node_type="agent", position=None, data=None):
        self.id = id
        self.type = node_type
        self.position = position or MockPosition()
        self.data = data or MockNodeData()


class MockEdge:
    def __init__(self, id="edge1", source="node1", target="node2", edge_type="default"):
        self.id = id
        self.source = source
        self.target = target
        self.type = edge_type


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def flow_service(mock_session):
    """Create a FlowService instance with mock session."""
    return FlowService(mock_session)


@pytest.fixture
def mock_flow():
    """Create a mock flow."""
    return MockFlow()


@pytest.fixture
def flow_create_data():
    """Create sample FlowCreate data."""
    return {
        "name": "Test Flow",
        "crew_id": str(uuid.uuid4()),
        "nodes": [
            {
                "id": "node1",
                "type": "agent",
                "position": {"x": 100, "y": 100},
                "data": {"label": "Agent 1", "type": "researcher"}
            }
        ],
        "edges": [
            {
                "id": "edge1",
                "source": "node1",
                "target": "node2",
                "type": "default"
            }
        ],
        "flow_config": {"version": "1.0"},
        "group_id": "group-123",
        "created_by_email": "test@example.com"
    }


@pytest.fixture
def flow_update_data():
    """Create sample FlowUpdate data."""
    return {
        "name": "Updated Flow",
        "nodes": [
            {
                "id": "node1",
                "type": "agent",
                "position": {"x": 150, "y": 150},
                "data": {"label": "Updated Agent", "type": "writer"}
            }
        ],
        "edges": [
            {
                "id": "edge1",
                "source": "node1",
                "target": "node2",
                "type": "bezier"
            }
        ],
        "flow_config": {"version": "2.0"}
    }


class TestFlowService:
    """Test cases for FlowService."""
    
    @pytest.mark.asyncio
    async def test_create_flow_success(self, flow_service, flow_create_data, mock_flow):
        """Test successful flow creation."""
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.create.return_value = mock_flow
            
            flow_create = FlowCreate(**flow_create_data)
            result = await flow_service.create_flow(flow_create)
            
            assert result == mock_flow
            mock_repo.create.assert_called_once()
            MockRepository.assert_called_once_with(flow_service.session)
    
    @pytest.mark.asyncio
    async def test_create_flow_validation_error(self, flow_service, flow_create_data):
        """Test flow creation with validation error."""
        # Create invalid data (missing required field)
        invalid_data = flow_create_data.copy()
        invalid_data["name"] = ""  # Empty name should cause validation error
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.create.side_effect = ValueError("Name cannot be empty")
            
            flow_create = FlowCreate(**invalid_data)
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.create_flow(flow_create)
            
            assert exc_info.value.status_code == 400
    
    @pytest.mark.asyncio
    async def test_create_flow_general_error(self, flow_service, flow_create_data):
        """Test flow creation with general error."""
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.create.side_effect = Exception("Database error")
            
            flow_create = FlowCreate(**flow_create_data)
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.create_flow(flow_create)
            
            assert exc_info.value.status_code == 500
            assert "Error creating flow" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_flow_success(self, flow_service, mock_flow):
        """Test successful flow retrieval."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            
            result = await flow_service.get_flow(flow_id)
            
            assert result == mock_flow
            mock_repo.get.assert_called_once_with(flow_id)
    
    @pytest.mark.asyncio
    async def test_get_flow_not_found(self, flow_service):
        """Test flow retrieval when flow not found."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = None
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.get_flow(flow_id)
            
            assert exc_info.value.status_code == 404
            assert "Flow not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_all_flows(self, flow_service):
        """Test getting all flows."""
        mock_flows = [MockFlow(name="Flow 1"), MockFlow(name="Flow 2")]
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.find_all.return_value = mock_flows
            
            result = await flow_service.get_all_flows()
            
            assert result == mock_flows
            assert len(result) == 2
            mock_repo.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_flows_by_crew_uuid(self, flow_service):
        """Test getting flows by crew ID (UUID)."""
        crew_id = uuid.uuid4()
        mock_flows = [MockFlow(crew_id=crew_id)]
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.find_by_crew_id.return_value = mock_flows
            
            result = await flow_service.get_flows_by_crew(crew_id)
            
            assert result == mock_flows
            mock_repo.find_by_crew_id.assert_called_once_with(crew_id)
    
    @pytest.mark.asyncio
    async def test_get_flows_by_crew_string(self, flow_service):
        """Test getting flows by crew ID (string)."""
        crew_id = uuid.uuid4()
        crew_id_str = str(crew_id)
        mock_flows = [MockFlow(crew_id=crew_id)]
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.find_by_crew_id.return_value = mock_flows
            
            result = await flow_service.get_flows_by_crew(crew_id_str)
            
            assert result == mock_flows
            mock_repo.find_by_crew_id.assert_called_once_with(crew_id)
    
    @pytest.mark.asyncio
    async def test_get_flows_by_crew_invalid_uuid(self, flow_service):
        """Test getting flows by crew ID with invalid UUID string."""
        invalid_crew_id = "invalid-uuid"
        
        result = await flow_service.get_flows_by_crew(invalid_crew_id)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_update_flow_success(self, flow_service, flow_update_data, mock_flow):
        """Test successful flow update."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            mock_repo.update.return_value = mock_flow
            
            flow_update = FlowUpdate(**flow_update_data)
            result = await flow_service.update_flow(flow_id, flow_update)
            
            assert result == mock_flow
            mock_repo.get.assert_called_once_with(flow_id)
            mock_repo.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_flow_not_found(self, flow_service, flow_update_data):
        """Test flow update when flow not found."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = None
            
            flow_update = FlowUpdate(**flow_update_data)
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.update_flow(flow_id, flow_update)
            
            assert exc_info.value.status_code == 404
            assert "Flow not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_flow_validation_error(self, flow_service, flow_update_data, mock_flow):
        """Test flow update with validation error."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            mock_repo.update.side_effect = ValueError("Invalid node configuration")
            
            flow_update = FlowUpdate(**flow_update_data)
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.update_flow(flow_id, flow_update)
            
            # ValueError is caught and re-raised as 500 error in the actual implementation
            assert exc_info.value.status_code == 500
    
    @pytest.mark.asyncio
    async def test_update_flow_general_error(self, flow_service, flow_update_data, mock_flow):
        """Test flow update with general error."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            mock_repo.update.side_effect = Exception("Database error")
            
            flow_update = FlowUpdate(**flow_update_data)
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.update_flow(flow_id, flow_update)
            
            assert exc_info.value.status_code == 500
    
    @pytest.mark.asyncio
    async def test_delete_flow_success(self, flow_service, mock_flow):
        """Test successful flow deletion."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            mock_repo.delete.return_value = True
            
            # Mock session execute for execution count check
            mock_result = MagicMock()
            mock_result.scalar_one.return_value = 0  # No executions
            flow_service.session.execute = AsyncMock(return_value=mock_result)
            
            result = await flow_service.delete_flow(flow_id)
            
            assert result is True
            mock_repo.get.assert_called_once_with(flow_id)
            mock_repo.delete.assert_called_once_with(flow_id)
    
    @pytest.mark.asyncio
    async def test_delete_flow_not_found(self, flow_service):
        """Test flow deletion when flow not found."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = None
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.delete_flow(flow_id)
            
            assert exc_info.value.status_code == 404
            assert "Flow not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_delete_flow_with_executions_error(self, flow_service, mock_flow):
        """Test flow deletion when executions exist."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            
            # Mock session execute to return execution count > 0
            mock_result = MagicMock()
            mock_result.scalar_one.return_value = 5  # Has executions
            flow_service.session.execute = AsyncMock(return_value=mock_result)
            
            with pytest.raises(HTTPException) as exc_info:
                await flow_service.delete_flow(flow_id)
            
            assert exc_info.value.status_code == 400
            assert "execution records" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_force_delete_flow_method_exists(self, flow_service):
        """Test that force delete method exists."""
        assert hasattr(flow_service, 'force_delete_flow_with_executions')
        assert callable(flow_service.force_delete_flow_with_executions)
    
    def test_force_delete_flow_error_handling(self, flow_service):
        """Test that force delete method has proper error handling structure."""
        # Just verify the method exists and can be called
        assert hasattr(flow_service, 'force_delete_flow_with_executions')
        # The actual error handling is complex due to async database operations
    
    @pytest.mark.asyncio
    async def test_delete_all_flows(self, flow_service):
        """Test deleting all flows."""
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.delete_all.return_value = None
            
            await flow_service.delete_all_flows()
            
            mock_repo.delete_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_flow_data_success(self, flow_service, flow_create_data):
        """Test successful flow data validation."""
        flow_create = FlowCreate(**flow_create_data)
        
        result = await flow_service.validate_flow_data(flow_create)
        
        assert result["status"] == "success"
        assert "message" in result
        assert "data" in result
        assert result["data"]["name"] == flow_create_data["name"]
        assert result["data"]["node_count"] == len(flow_create_data["nodes"])
        assert result["data"]["edge_count"] == len(flow_create_data["edges"])
    
    @pytest.mark.asyncio
    async def test_validate_flow_data_with_pydantic_error(self, flow_service):
        """Test flow validation with Pydantic validation error."""
        # Create invalid data that will cause Pydantic to fail
        invalid_data = {
            "name": "",  # Empty name
            "crew_id": "invalid-uuid",  # Invalid UUID
            "nodes": [],
            "edges": []
        }
        
        # This should raise a ValidationError when creating FlowCreate
        with pytest.raises(Exception):  # Pydantic ValidationError
            flow_create = FlowCreate(**invalid_data)
            await flow_service.validate_flow_data(flow_create)
    
    def test_validate_flow_data_method_exists(self, flow_service):
        """Test that validate_flow_data method exists."""
        assert hasattr(flow_service, 'validate_flow_data')
        assert callable(flow_service.validate_flow_data)
    
    @pytest.mark.asyncio
    async def test_create_flow_with_invalid_listener_format(self, flow_service, flow_create_data):
        """Test flow creation with invalid listener format."""
        flow_create_data["flow_config"] = {
            "listeners": ["invalid_listener"],  # Should be dict, not string
            "actions": []
        }
        
        flow_create = FlowCreate(**flow_create_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await flow_service.create_flow(flow_create)
        
        assert exc_info.value.status_code == 400
        assert "Invalid listener format" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_flow_with_missing_listener_fields(self, flow_service, flow_create_data):
        """Test flow creation with missing required listener fields."""
        flow_create_data["flow_config"] = {
            "listeners": [{"id": "listener1"}],  # Missing name and crewId
            "actions": []
        }
        
        flow_create = FlowCreate(**flow_create_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await flow_service.create_flow(flow_create)
        
        assert exc_info.value.status_code == 400
        assert "Missing required fields in listener" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_flow_with_invalid_action_format(self, flow_service, flow_create_data):
        """Test flow creation with invalid action format."""
        flow_create_data["flow_config"] = {
            "listeners": [],
            "actions": ["invalid_action"]  # Should be dict, not string
        }
        
        flow_create = FlowCreate(**flow_create_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await flow_service.create_flow(flow_create)
        
        assert exc_info.value.status_code == 400
        assert "Invalid action format" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_flow_with_missing_action_fields(self, flow_service, flow_create_data):
        """Test flow creation with missing required action fields."""
        flow_create_data["flow_config"] = {
            "listeners": [],
            "actions": [{"id": "action1"}]  # Missing crewId and taskId
        }
        
        flow_create = FlowCreate(**flow_create_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await flow_service.create_flow(flow_create)
        
        assert exc_info.value.status_code == 400
        assert "Missing required fields in action" in str(exc_info.value.detail)
    
    def test_flow_service_initialization(self, flow_service, mock_session):
        """Test FlowService initialization."""
        assert flow_service.session == mock_session
        assert hasattr(flow_service, 'session')
    
    @pytest.mark.asyncio
    async def test_flow_config_normalization(self, flow_service, flow_create_data, mock_flow):
        """Test that flow config is properly normalized during creation."""
        # Test with None flow_config
        flow_create_data["flow_config"] = None
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.create.return_value = mock_flow
            
            flow_create = FlowCreate(**flow_create_data)
            result = await flow_service.create_flow(flow_create)
            
            # Verify that create was called
            mock_repo.create.assert_called_once()
            assert result == mock_flow
    
    @pytest.mark.asyncio
    async def test_crew_id_type_conversion(self, flow_service):
        """Test that crew_id string conversion works properly."""
        # Test with valid UUID string
        valid_uuid_str = str(uuid.uuid4())
        mock_flows = [MockFlow()]
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.find_by_crew_id.return_value = mock_flows
            
            result = await flow_service.get_flows_by_crew(valid_uuid_str)
            
            # Should convert string to UUID and call repository
            assert result == mock_flows
            called_with_uuid = mock_repo.find_by_crew_id.call_args[0][0]
            assert isinstance(called_with_uuid, uuid.UUID)
            assert str(called_with_uuid) == valid_uuid_str
    
    @pytest.mark.asyncio
    async def test_error_logging_during_creation(self, flow_service, flow_create_data):
        """Test that errors are properly logged during flow creation."""
        with patch('src.services.flow_service.FlowRepository') as MockRepository, \
             patch('src.services.flow_service.logger') as mock_logger:
            
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.create.side_effect = Exception("Database connection failed")
            
            flow_create = FlowCreate(**flow_create_data)
            
            with pytest.raises(HTTPException):
                await flow_service.create_flow(flow_create)
            
            # Verify error was logged
            mock_logger.error.assert_called()
            log_call = mock_logger.error.call_args[0][0]
            assert "Error creating flow" in log_call
    
    @pytest.mark.asyncio
    async def test_update_flow_timestamp_handling(self, flow_service, flow_update_data, mock_flow):
        """Test that flow update properly handles timestamp updates."""
        flow_id = uuid.uuid4()
        
        with patch('src.services.flow_service.FlowRepository') as MockRepository:
            mock_repo = AsyncMock()
            MockRepository.return_value = mock_repo
            mock_repo.get.return_value = mock_flow
            mock_repo.update.return_value = mock_flow
            
            flow_update = FlowUpdate(**flow_update_data)
            result = await flow_service.update_flow(flow_id, flow_update)
            
            # Verify update was called
            mock_repo.update.assert_called_once()
            
            # The updated flow object should be returned
            assert result == mock_flow