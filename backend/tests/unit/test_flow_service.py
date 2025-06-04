"""
Unit tests for FlowService.

Tests the functionality of the flow service including
creating, updating, deleting, and managing flows.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.flow_service import FlowService
from src.schemas.flow import FlowCreate, FlowUpdate
from src.models.flow import Flow
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    uow.session = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_flow_repository():
    """Create a mock flow repository."""
    repo = AsyncMock()
    
    # Create mock flow objects
    mock_flow = MagicMock(spec=Flow)
    mock_flow.id = uuid.uuid4()
    mock_flow.name = "Test Flow"
    mock_flow.description = "Test Flow Description"
    mock_flow.nodes = [{"id": "node1", "type": "agent", "data": {}}]
    mock_flow.edges = [{"source": "node1", "target": "node2", "data": {}}]
    mock_flow.flow_config = {"setting": "value"}
    mock_flow.created_at = datetime.now(UTC)
    mock_flow.updated_at = datetime.now(UTC)
    mock_flow.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_flow
    repo.list.return_value = [mock_flow]
    repo.create.return_value = mock_flow
    repo.update.return_value = mock_flow
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_flow
    repo.search.return_value = [mock_flow]
    
    return repo


@pytest.fixture
def flow_create_data():
    """Create test data for flow creation."""
    return FlowCreate(
        name="Test Flow",
        description="A test flow for unit testing",
        nodes=[
            {"id": "node1", "type": "agent", "data": {"name": "Agent 1"}},
            {"id": "node2", "type": "task", "data": {"name": "Task 1"}}
        ],
        edges=[
            {"source": "node1", "target": "node2", "data": {}}
        ],
        flow_config={"timeout": 300, "retries": 3}
    )


@pytest.fixture
def flow_update_data():
    """Create test data for flow updates."""
    return FlowUpdate(
        name="Updated Flow",
        description="Updated description",
        flow_config={"timeout": 600, "retries": 5}
    )


class TestFlowService:
    """Test cases for FlowService."""
    
    @pytest.mark.asyncio
    async def test_create_flow_success(self, mock_uow, mock_flow_repository, flow_create_data):
        """Test successful flow creation."""
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.create(flow_create_data)
            
            assert result is not None
            assert result.name == "Test Flow"
            mock_flow_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_flow_validation_error(self, mock_uow, mock_flow_repository):
        """Test flow creation with invalid data."""
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            # Test with invalid data (empty name)
            invalid_data = FlowCreate(
                name="",  # Empty name should fail validation
                description="Test",
                nodes=[],
                edges=[],
                flow_config={}
            )
            
            mock_flow_repository.create.side_effect = ValueError("Name cannot be empty")
            
            with pytest.raises(ValueError, match="Name cannot be empty"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_flow_by_id(self, mock_uow, mock_flow_repository):
        """Test getting a flow by ID."""
        flow_id = uuid.uuid4()
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.get_flow(flow_id)
            
            assert result is not None
            assert result.name == "Test Flow"
            mock_flow_repository.get.assert_called_once_with(flow_id)
    
    @pytest.mark.asyncio
    async def test_get_flow_not_found(self, mock_uow, mock_flow_repository):
        """Test getting a non-existent flow."""
        flow_id = uuid.uuid4()
        mock_flow_repository.get.return_value = None
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            with pytest.raises(Exception, match="Flow not found"):
                await service.get_flow(flow_id)
    
    @pytest.mark.asyncio
    async def test_update_flow_success(self, mock_uow, mock_flow_repository, flow_update_data):
        """Test successful flow update."""
        flow_id = uuid.uuid4()
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.update(flow_id, flow_update_data)
            
            assert result is not None
            mock_flow_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_flow_not_found(self, mock_uow, mock_flow_repository, flow_update_data):
        """Test updating a non-existent flow."""
        flow_id = uuid.uuid4()
        mock_flow_repository.update.return_value = None
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.update(flow_id, flow_update_data)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_delete_flow_success(self, mock_uow, mock_flow_repository):
        """Test successful flow deletion."""
        flow_id = uuid.uuid4()
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.delete(flow_id)
            
            assert result is True
            mock_flow_repository.delete.assert_called_once_with(flow_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_flow_not_found(self, mock_uow, mock_flow_repository):
        """Test deleting a non-existent flow."""
        flow_id = uuid.uuid4()
        mock_flow_repository.delete.return_value = False
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.delete(flow_id)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_list_flows(self, mock_uow, mock_flow_repository):
        """Test listing all flows."""
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].name == "Test Flow"
            mock_flow_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_flow_by_name(self, mock_uow, mock_flow_repository):
        """Test getting a flow by name."""
        flow_name = "Test Flow"
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.get_by_name(flow_name)
            
            assert result is not None
            assert result.name == flow_name
            mock_flow_repository.get_by_name.assert_called_once_with(flow_name)
    
    @pytest.mark.asyncio
    async def test_search_flows(self, mock_uow, mock_flow_repository):
        """Test searching flows."""
        search_query = "test"
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            result = await service.search(search_query)
            
            assert len(result) == 1
            assert result[0].name == "Test Flow"
            mock_flow_repository.search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_validate_flow_structure(self, mock_uow):
        """Test validation of flow node and edge structure."""
        service = FlowService(mock_uow)
        
        # Test valid structure
        valid_nodes = [
            {"id": "node1", "type": "agent", "data": {"name": "Agent 1"}},
            {"id": "node2", "type": "task", "data": {"name": "Task 1"}}
        ]
        valid_edges = [
            {"source": "node1", "target": "node2", "data": {}}
        ]
        
        # Should not raise exception
        service._validate_flow_structure(valid_nodes, valid_edges)
        
        # Test invalid structure (edge references non-existent node)
        invalid_edges = [
            {"source": "node1", "target": "node3", "data": {}}  # node3 doesn't exist
        ]
        
        with pytest.raises(ValueError, match="Edge references non-existent node"):
            service._validate_flow_structure(valid_nodes, invalid_edges)
    
    @pytest.mark.asyncio
    async def test_validate_node_types(self, mock_uow):
        """Test validation of node types."""
        service = FlowService(mock_uow)
        
        # Test valid node types
        valid_nodes = [
            {"id": "node1", "type": "agent", "data": {}},
            {"id": "node2", "type": "task", "data": {}},
            {"id": "node3", "type": "condition", "data": {}}
        ]
        
        service._validate_node_types(valid_nodes)
        
        # Test invalid node type
        invalid_nodes = [
            {"id": "node1", "type": "invalid_type", "data": {}}
        ]
        
        with pytest.raises(ValueError, match="Invalid node type"):
            service._validate_node_types(invalid_nodes)
    
    @pytest.mark.asyncio
    async def test_validate_flow_connectivity(self, mock_uow):
        """Test validation of flow connectivity."""
        service = FlowService(mock_uow)
        
        # Test connected flow
        nodes = [
            {"id": "node1", "type": "agent", "data": {}},
            {"id": "node2", "type": "task", "data": {}},
            {"id": "node3", "type": "task", "data": {}}
        ]
        edges = [
            {"source": "node1", "target": "node2", "data": {}},
            {"source": "node2", "target": "node3", "data": {}}
        ]
        
        service._validate_flow_connectivity(nodes, edges)
        
        # Test disconnected flow
        disconnected_nodes = [
            {"id": "node1", "type": "agent", "data": {}},
            {"id": "node2", "type": "task", "data": {}},
            {"id": "node3", "type": "task", "data": {}}  # Not connected
        ]
        disconnected_edges = [
            {"source": "node1", "target": "node2", "data": {}}
        ]
        
        with pytest.raises(ValueError, match="Flow contains disconnected nodes"):
            service._validate_flow_connectivity(disconnected_nodes, disconnected_edges)
    
    @pytest.mark.asyncio
    async def test_detect_cycles(self, mock_uow):
        """Test cycle detection in flow."""
        service = FlowService(mock_uow)
        
        # Test acyclic flow
        acyclic_edges = [
            {"source": "node1", "target": "node2", "data": {}},
            {"source": "node2", "target": "node3", "data": {}}
        ]
        
        assert not service._has_cycles(["node1", "node2", "node3"], acyclic_edges)
        
        # Test cyclic flow
        cyclic_edges = [
            {"source": "node1", "target": "node2", "data": {}},
            {"source": "node2", "target": "node3", "data": {}},
            {"source": "node3", "target": "node1", "data": {}}  # Creates cycle
        ]
        
        assert service._has_cycles(["node1", "node2", "node3"], cyclic_edges)
    
    @pytest.mark.asyncio
    async def test_flow_activation_deactivation(self, mock_uow, mock_flow_repository):
        """Test flow activation and deactivation."""
        flow_id = uuid.uuid4()
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            # Test activation
            result = await service.activate(flow_id)
            assert result is not None
            
            # Test deactivation
            result = await service.deactivate(flow_id)
            assert result is not None
            
            # Verify repository calls
            assert mock_flow_repository.update.call_count == 2
    
    @pytest.mark.asyncio
    async def test_flow_export_import(self, mock_uow, mock_flow_repository):
        """Test flow export and import functionality."""
        flow_id = uuid.uuid4()
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            # Test export
            export_data = await service.export_flow(flow_id)
            
            assert export_data is not None
            assert "name" in export_data
            assert "nodes" in export_data
            assert "edges" in export_data
            assert "flow_config" in export_data
            
            # Test import
            imported_flow = await service.import_flow(export_data)
            
            assert imported_flow is not None
            mock_flow_repository.create.assert_called()
    
    @pytest.mark.asyncio
    async def test_duplicate_flow_name(self, mock_uow, mock_flow_repository, flow_create_data):
        """Test creating flow with duplicate name."""
        # Mock repository to return existing flow with same name
        mock_flow_repository.get_by_name.return_value = MagicMock()
        mock_flow_repository.create.side_effect = ValueError("Flow name already exists")
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            with pytest.raises(ValueError, match="Flow name already exists"):
                await service.create(flow_create_data)
    
    @pytest.mark.asyncio
    async def test_flow_clone(self, mock_uow, mock_flow_repository):
        """Test flow cloning functionality."""
        flow_id = uuid.uuid4()
        new_name = "Cloned Flow"
        
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            cloned_flow = await service.clone_flow(flow_id, new_name)
            
            assert cloned_flow is not None
            mock_flow_repository.get.assert_called_once_with(flow_id)
            mock_flow_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_flow_metrics(self, mock_uow, mock_flow_repository):
        """Test flow metrics collection."""
        with patch("src.services.flow_service.FlowRepository", return_value=mock_flow_repository):
            service = FlowService(mock_uow)
            
            metrics = await service.get_flow_metrics()
            
            assert "total_flows" in metrics
            assert "active_flows" in metrics
            assert metrics["total_flows"] >= 0
            assert metrics["active_flows"] >= 0
    
    @pytest.mark.asyncio
    async def test_flow_validation_comprehensive(self, mock_uow):
        """Test comprehensive flow validation."""
        service = FlowService(mock_uow)
        
        # Test valid complex flow
        complex_nodes = [
            {"id": "start", "type": "agent", "data": {"name": "Start Agent"}},
            {"id": "condition", "type": "condition", "data": {"condition": "x > 0"}},
            {"id": "task1", "type": "task", "data": {"name": "Task 1"}},
            {"id": "task2", "type": "task", "data": {"name": "Task 2"}},
            {"id": "end", "type": "task", "data": {"name": "End Task"}}
        ]
        
        complex_edges = [
            {"source": "start", "target": "condition", "data": {}},
            {"source": "condition", "target": "task1", "data": {"condition": "true"}},
            {"source": "condition", "target": "task2", "data": {"condition": "false"}},
            {"source": "task1", "target": "end", "data": {}},
            {"source": "task2", "target": "end", "data": {}}
        ]
        
        # Should not raise exception
        service._validate_flow_structure(complex_nodes, complex_edges)
        
        # Test validation of required node data
        invalid_nodes = [
            {"id": "node1", "type": "agent"}  # Missing data field
        ]
        
        with pytest.raises(ValueError, match="Node missing required data field"):
            service._validate_node_data(invalid_nodes)