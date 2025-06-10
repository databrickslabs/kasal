"""
Unit tests for CrewService.

Tests the functionality of the crew service including
creating, updating, deleting, and managing crews.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.crew_service import CrewService
from src.schemas.crew import CrewCreate, CrewUpdate
from src.models.crew import Crew
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
def mock_crew_repository():
    """Create a mock crew repository."""
    repo = AsyncMock()
    
    # Create mock crew objects
    mock_crew = MagicMock(spec=Crew)
    mock_crew.id = uuid.uuid4()
    mock_crew.name = "Test Crew"
    mock_crew.description = "Test Description"
    mock_crew.agents_yaml = {"agent1": {"role": "researcher"}}
    mock_crew.tasks_yaml = {"task1": {"description": "research task"}}
    mock_crew.created_at = datetime.now(UTC)
    mock_crew.updated_at = datetime.now(UTC)
    mock_crew.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_crew
    repo.list.return_value = [mock_crew]
    repo.create.return_value = mock_crew
    repo.update.return_value = mock_crew
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_crew
    repo.search.return_value = [mock_crew]
    
    return repo


@pytest.fixture
def crew_create_data():
    """Create test data for crew creation."""
    return CrewCreate(
        name="Test Crew",
        description="A test crew for unit testing",
        agents_yaml={"agent1": {"role": "researcher", "goal": "research"}},
        tasks_yaml={"task1": {"description": "research task", "agent": "agent1"}}
    )


@pytest.fixture
def crew_update_data():
    """Create test data for crew updates."""
    return CrewUpdate(
        name="Updated Crew",
        description="Updated description"
    )


class TestCrewService:
    """Test cases for CrewService."""
    
    @pytest.mark.asyncio
    async def test_create_crew_success(self, mock_uow, mock_crew_repository, crew_create_data):
        """Test successful crew creation."""
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.create(crew_create_data)
            
            assert result is not None
            assert result.name == "Test Crew"
            mock_crew_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_crew_validation_error(self, mock_uow, mock_crew_repository):
        """Test crew creation with invalid data."""
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            # Test with invalid data (missing required fields)
            invalid_data = CrewCreate(
                name="",  # Empty name should fail validation
                description="Test",
                agents_yaml={},
                tasks_yaml={}
            )
            
            mock_crew_repository.create.side_effect = ValueError("Name cannot be empty")
            
            with pytest.raises(ValueError, match="Name cannot be empty"):
                await service.create(invalid_data)
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_crew_by_id(self, mock_uow, mock_crew_repository):
        """Test getting a crew by ID."""
        crew_id = uuid.uuid4()
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.get(crew_id)
            
            assert result is not None
            assert result.name == "Test Crew"
            mock_crew_repository.get.assert_called_once_with(crew_id)
    
    @pytest.mark.asyncio
    async def test_get_crew_not_found(self, mock_uow, mock_crew_repository):
        """Test getting a non-existent crew."""
        crew_id = uuid.uuid4()
        mock_crew_repository.get.return_value = None
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.get(crew_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_crew_success(self, mock_uow, mock_crew_repository, crew_update_data):
        """Test successful crew update."""
        crew_id = uuid.uuid4()
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.update(crew_id, crew_update_data)
            
            assert result is not None
            mock_crew_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_crew_not_found(self, mock_uow, mock_crew_repository, crew_update_data):
        """Test updating a non-existent crew."""
        crew_id = uuid.uuid4()
        mock_crew_repository.update.return_value = None
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.update(crew_id, crew_update_data)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_delete_crew_success(self, mock_uow, mock_crew_repository):
        """Test successful crew deletion."""
        crew_id = uuid.uuid4()
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.delete(crew_id)
            
            assert result is True
            mock_crew_repository.delete.assert_called_once_with(crew_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_crew_not_found(self, mock_uow, mock_crew_repository):
        """Test deleting a non-existent crew."""
        crew_id = uuid.uuid4()
        mock_crew_repository.delete.return_value = False
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.delete(crew_id)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_list_crews(self, mock_uow, mock_crew_repository):
        """Test listing all crews."""
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].name == "Test Crew"
            mock_crew_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_crew_by_name(self, mock_uow, mock_crew_repository):
        """Test getting a crew by name."""
        crew_name = "Test Crew"
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.get_by_name(crew_name)
            
            assert result is not None
            assert result.name == crew_name
            mock_crew_repository.get_by_name.assert_called_once_with(crew_name)
    
    @pytest.mark.asyncio
    async def test_search_crews(self, mock_uow, mock_crew_repository):
        """Test searching crews."""
        search_query = "test"
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.search(search_query)
            
            assert len(result) == 1
            assert result[0].name == "Test Crew"
            mock_crew_repository.search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_validate_crew_yaml_structure(self, mock_uow):
        """Test validation of crew YAML structure."""
        service = CrewService(mock_uow)
        
        # Test valid structure
        valid_agents = {"agent1": {"role": "researcher", "goal": "research"}}
        valid_tasks = {"task1": {"description": "task", "agent": "agent1"}}
        
        # Should not raise exception
        service._validate_yaml_structure(valid_agents, valid_tasks)
        
        # Test invalid structure (missing required fields)
        invalid_agents = {"agent1": {"role": "researcher"}}  # Missing goal
        
        with pytest.raises(ValueError, match="Invalid agent structure"):
            service._validate_yaml_structure(invalid_agents, valid_tasks)
    
    @pytest.mark.asyncio
    async def test_crew_agents_consistency(self, mock_uow):
        """Test validation of agent-task consistency."""
        service = CrewService(mock_uow)
        
        agents = {"agent1": {"role": "researcher", "goal": "research"}}
        
        # Test valid task assignment
        valid_tasks = {"task1": {"description": "task", "agent": "agent1"}}
        service._validate_yaml_structure(agents, valid_tasks)
        
        # Test invalid task assignment (non-existent agent)
        invalid_tasks = {"task1": {"description": "task", "agent": "agent2"}}
        
        with pytest.raises(ValueError, match="Task references non-existent agent"):
            service._validate_yaml_structure(agents, invalid_tasks)
    
    @pytest.mark.asyncio
    async def test_duplicate_crew_name(self, mock_uow, mock_crew_repository, crew_create_data):
        """Test creating crew with duplicate name."""
        # Mock repository to return existing crew with same name
        mock_crew_repository.get_by_name.return_value = MagicMock()
        mock_crew_repository.create.side_effect = ValueError("Crew name already exists")
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            with pytest.raises(ValueError, match="Crew name already exists"):
                await service.create(crew_create_data)
    
    @pytest.mark.asyncio
    async def test_crew_yaml_serialization(self, mock_uow, mock_crew_repository):
        """Test YAML serialization/deserialization."""
        crew_data = CrewCreate(
            name="YAML Test Crew",
            description="Test YAML handling",
            agents_yaml={"agent1": {"role": "researcher", "goal": "research"}},
            tasks_yaml={"task1": {"description": "task", "agent": "agent1"}}
        )
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            result = await service.create(crew_data)
            
            # Verify that YAML data is properly handled
            create_call_args = mock_crew_repository.create.call_args[0][0]
            assert "agents_yaml" in create_call_args
            assert "tasks_yaml" in create_call_args
            assert isinstance(create_call_args["agents_yaml"], dict)
            assert isinstance(create_call_args["tasks_yaml"], dict)
    
    @pytest.mark.asyncio
    async def test_crew_activation_deactivation(self, mock_uow, mock_crew_repository):
        """Test crew activation and deactivation."""
        crew_id = uuid.uuid4()
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            # Test activation
            result = await service.activate(crew_id)
            assert result is not None
            
            # Test deactivation
            result = await service.deactivate(crew_id)
            assert result is not None
            
            # Verify repository calls
            assert mock_crew_repository.update.call_count == 2
    
    @pytest.mark.asyncio
    async def test_crew_export_import(self, mock_uow, mock_crew_repository):
        """Test crew export and import functionality."""
        crew_id = uuid.uuid4()
        
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            # Test export
            export_data = await service.export_crew(crew_id)
            
            assert export_data is not None
            assert "name" in export_data
            assert "agents_yaml" in export_data
            assert "tasks_yaml" in export_data
            
            # Test import
            imported_crew = await service.import_crew(export_data)
            
            assert imported_crew is not None
            mock_crew_repository.create.assert_called()
    
    @pytest.mark.asyncio
    async def test_crew_metrics(self, mock_uow, mock_crew_repository):
        """Test crew metrics collection."""
        with patch("src.services.crew_service.CrewRepository", return_value=mock_crew_repository):
            service = CrewService(mock_uow)
            
            metrics = await service.get_crew_metrics()
            
            assert "total_crews" in metrics
            assert "active_crews" in metrics
            assert metrics["total_crews"] >= 0
            assert metrics["active_crews"] >= 0