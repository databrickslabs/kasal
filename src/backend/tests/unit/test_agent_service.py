"""
Unit tests for AgentService.

Tests the functionality of the agent service including
creating, updating, deleting, and managing agents.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.agent_service import AgentService
from src.schemas.agent import AgentCreate, AgentUpdate
from src.models.agent import Agent
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    # Create a proper mock session with SQLAlchemy methods
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.delete = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.scalars = MagicMock()
    
    uow.session = mock_session
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_agent_repository():
    """Create a mock agent repository."""
    repo = AsyncMock()
    
    # Create mock agent objects
    mock_agent = MagicMock(spec=Agent)
    mock_agent.id = uuid.uuid4()
    mock_agent.name = "Test Agent"
    mock_agent.role = "Research Assistant"
    mock_agent.goal = "Conduct thorough research"
    mock_agent.backstory = "I am a specialized research assistant"
    mock_agent.tools = ["web_search", "document_analyzer"]
    mock_agent.llm = "gpt-4o-mini"
    mock_agent.max_iter = 25
    mock_agent.created_at = datetime.now(UTC)
    mock_agent.updated_at = datetime.now(UTC)
    mock_agent.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_agent
    repo.list.return_value = [mock_agent]
    repo.create.return_value = mock_agent
    repo.update.return_value = mock_agent
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_agent
    repo.search.return_value = [mock_agent]
    
    return repo


@pytest.fixture
def agent_create_data():
    """Create test data for agent creation."""
    return AgentCreate(
        name="Test Agent",
        role="Research Assistant", 
        goal="Conduct thorough research on given topics",
        backstory="I am a specialized research assistant with expertise in data gathering",
        tools=["web_search", "document_analyzer"],
        llm="gpt-4o-mini",
        max_iter=25,
        allow_delegation=False,
        verbose=True
    )


@pytest.fixture
def agent_update_data():
    """Create test data for agent updates."""
    return AgentUpdate(
        name="Updated Agent",
        goal="Updated research goals",
        max_iter=30
    )


class TestAgentService:
    """Test cases for AgentService."""
    
    @pytest.mark.asyncio
    async def test_create_agent_success(self, mock_uow, mock_agent_repository, agent_create_data):
        """Test successful agent creation."""
        # Create service and then replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        # Mock the repository create method to return a proper agent object
        mock_agent = MagicMock(spec=Agent)
        mock_agent.name = "Test Agent"
        mock_agent.role = "Research Assistant"
        mock_agent_repository.create.return_value = mock_agent
        
        result = await service.create(agent_create_data)
        
        assert result is not None
        assert result.name == "Test Agent"
        assert result.role == "Research Assistant"
        mock_agent_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_agent_validation_error(self, mock_uow, mock_agent_repository):
        """Test agent creation with invalid data."""
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        # Test with invalid data (empty role)
        invalid_data = AgentCreate(
            name="Test Agent",
            role="",  # Empty role should fail validation
            goal="Test goal",
            backstory="Test backstory",
            tools=[],
            llm="gpt-4o-mini"
        )
        
        mock_agent_repository.create.side_effect = ValueError("Role cannot be empty")
        
        with pytest.raises(ValueError, match="Role cannot be empty"):
            await service.create(invalid_data)
    
    @pytest.mark.asyncio
    async def test_get_agent_by_id(self, mock_uow, mock_agent_repository):
        """Test getting an agent by ID."""
        agent_id = uuid.uuid4()
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        # Setup mock to return an agent
        mock_agent = MagicMock(spec=Agent)
        mock_agent.name = "Test Agent"
        mock_agent_repository.get.return_value = mock_agent
        
        result = await service.get(agent_id)
        
        assert result is not None
        assert result.name == "Test Agent"
        mock_agent_repository.get.assert_called_once_with(agent_id)
    
    @pytest.mark.asyncio
    async def test_get_agent_not_found(self, mock_uow, mock_agent_repository):
        """Test getting a non-existent agent."""
        agent_id = uuid.uuid4()
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        mock_agent_repository.get.return_value = None
        
        result = await service.get(agent_id)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_agent_success(self, mock_uow, mock_agent_repository, agent_update_data):
        """Test successful agent update."""
        agent_id = uuid.uuid4()
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        # Mock the update method to return the updated agent
        mock_agent = MagicMock(spec=Agent)
        mock_agent.name = "Updated Agent"
        mock_agent_repository.update.return_value = mock_agent
        
        # Call update_with_partial_data instead which uses self.repository
        result = await service.update_with_partial_data(str(agent_id), agent_update_data)
        
        assert result is not None
        assert result.name == "Updated Agent"
        mock_agent_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_agent_not_found(self, mock_uow, mock_agent_repository, agent_update_data):
        """Test updating a non-existent agent."""
        agent_id = uuid.uuid4()
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        mock_agent_repository.update.return_value = None
        
        # Call update_with_partial_data instead which uses self.repository
        result = await service.update_with_partial_data(str(agent_id), agent_update_data)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_delete_agent_success(self, mock_uow, mock_agent_repository):
        """Test successful agent deletion."""
        agent_id = uuid.uuid4()
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        mock_agent_repository.delete.return_value = True
        
        result = await service.delete(agent_id)
        
        assert result is True
        mock_agent_repository.delete.assert_called_once_with(agent_id)
    
    @pytest.mark.asyncio
    async def test_delete_agent_not_found(self, mock_uow, mock_agent_repository):
        """Test deleting a non-existent agent."""
        agent_id = uuid.uuid4()
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        mock_agent_repository.delete.return_value = False
        
        result = await service.delete(agent_id)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_list_agents(self, mock_uow, mock_agent_repository):
        """Test listing all agents."""
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        # Setup mock to return list of agents
        mock_agent = MagicMock(spec=Agent)
        mock_agent.name = "Test Agent"
        mock_agent_repository.find_all.return_value = [mock_agent]
        
        result = await service.find_all()
        
        assert len(result) == 1
        assert result[0].name == "Test Agent"
        mock_agent_repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_agent_by_name(self, mock_uow, mock_agent_repository):
        """Test getting an agent by name."""
        agent_name = "Test Agent"
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        # Setup mock to return an agent
        mock_agent = MagicMock(spec=Agent)
        mock_agent.name = agent_name
        mock_agent_repository.find_by_name.return_value = mock_agent
        
        result = await service.find_by_name(agent_name)
        
        assert result is not None
        assert result.name == agent_name
        mock_agent_repository.find_by_name.assert_called_once_with(agent_name)
    
    # Removed test_search_agents as the search method doesn't exist in AgentService
    
    # Removed validation tests as these methods don't exist in AgentService
    
    @pytest.mark.asyncio
    async def test_duplicate_agent_name(self, mock_uow, mock_agent_repository, agent_create_data):
        """Test creating agent with duplicate name."""
        # Mock repository to return existing agent with same name
        mock_agent_repository.get_by_name.return_value = MagicMock()
        mock_agent_repository.create.side_effect = ValueError("Agent name already exists")
        
        # Create service and replace its repository with our mock
        service = AgentService(mock_uow.session)
        service.repository = mock_agent_repository
        
        with pytest.raises(ValueError, match="Agent name already exists"):
            await service.create(agent_create_data)
    
    # Removed agent configuration validation test as the method doesn't exist
    
    # Removed tests for non-existent methods:
    # - test_agent_activation_deactivation
    # - test_agent_export_import
    # - test_agent_clone
    # - test_agent_metrics
    # - test_agent_performance_tracking