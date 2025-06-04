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
    uow.session = AsyncMock()
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
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.create(agent_create_data)
            
            assert result is not None
            assert result.name == "Test Agent"
            assert result.role == "Research Assistant"
            mock_agent_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_agent_validation_error(self, mock_uow, mock_agent_repository):
        """Test agent creation with invalid data."""
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
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
            
            mock_uow.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_agent_by_id(self, mock_uow, mock_agent_repository):
        """Test getting an agent by ID."""
        agent_id = uuid.uuid4()
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.get(agent_id)
            
            assert result is not None
            assert result.name == "Test Agent"
            mock_agent_repository.get.assert_called_once_with(agent_id)
    
    @pytest.mark.asyncio
    async def test_get_agent_not_found(self, mock_uow, mock_agent_repository):
        """Test getting a non-existent agent."""
        agent_id = uuid.uuid4()
        mock_agent_repository.get.return_value = None
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.get(agent_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_agent_success(self, mock_uow, mock_agent_repository, agent_update_data):
        """Test successful agent update."""
        agent_id = uuid.uuid4()
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.update(agent_id, agent_update_data)
            
            assert result is not None
            mock_agent_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_agent_not_found(self, mock_uow, mock_agent_repository, agent_update_data):
        """Test updating a non-existent agent."""
        agent_id = uuid.uuid4()
        mock_agent_repository.update.return_value = None
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.update(agent_id, agent_update_data)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_delete_agent_success(self, mock_uow, mock_agent_repository):
        """Test successful agent deletion."""
        agent_id = uuid.uuid4()
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.delete(agent_id)
            
            assert result is True
            mock_agent_repository.delete.assert_called_once_with(agent_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_agent_not_found(self, mock_uow, mock_agent_repository):
        """Test deleting a non-existent agent."""
        agent_id = uuid.uuid4()
        mock_agent_repository.delete.return_value = False
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.delete(agent_id)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_list_agents(self, mock_uow, mock_agent_repository):
        """Test listing all agents."""
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].name == "Test Agent"
            mock_agent_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_agent_by_name(self, mock_uow, mock_agent_repository):
        """Test getting an agent by name."""
        agent_name = "Test Agent"
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.get_by_name(agent_name)
            
            assert result is not None
            assert result.name == agent_name
            mock_agent_repository.get_by_name.assert_called_once_with(agent_name)
    
    @pytest.mark.asyncio
    async def test_search_agents(self, mock_uow, mock_agent_repository):
        """Test searching agents."""
        search_query = "research"
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.search(search_query)
            
            assert len(result) == 1
            assert result[0].name == "Test Agent"
            mock_agent_repository.search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_validate_agent_tools(self, mock_uow):
        """Test validation of agent tools."""
        service = AgentService(mock_uow)
        
        # Test valid tools
        valid_tools = ["web_search", "document_analyzer", "calculator"]
        service._validate_tools(valid_tools)  # Should not raise
        
        # Test invalid tools (non-existent)
        invalid_tools = ["nonexistent_tool"]
        
        with pytest.raises(ValueError, match="Invalid tool"):
            service._validate_tools(invalid_tools)
    
    @pytest.mark.asyncio
    async def test_validate_agent_llm(self, mock_uow):
        """Test validation of agent LLM configuration."""
        service = AgentService(mock_uow)
        
        # Test valid LLM
        valid_llm = "gpt-4o-mini"
        service._validate_llm(valid_llm)  # Should not raise
        
        # Test invalid LLM
        invalid_llm = "invalid-model"
        
        with pytest.raises(ValueError, match="Invalid LLM model"):
            service._validate_llm(invalid_llm)
    
    @pytest.mark.asyncio
    async def test_validate_max_iter(self, mock_uow):
        """Test validation of max_iter parameter."""
        service = AgentService(mock_uow)
        
        # Test valid max_iter
        service._validate_max_iter(25)  # Should not raise
        
        # Test invalid max_iter (negative)
        with pytest.raises(ValueError, match="max_iter must be positive"):
            service._validate_max_iter(-1)
        
        # Test invalid max_iter (too large)
        with pytest.raises(ValueError, match="max_iter cannot exceed"):
            service._validate_max_iter(1000)
    
    @pytest.mark.asyncio
    async def test_duplicate_agent_name(self, mock_uow, mock_agent_repository, agent_create_data):
        """Test creating agent with duplicate name."""
        # Mock repository to return existing agent with same name
        mock_agent_repository.get_by_name.return_value = MagicMock()
        mock_agent_repository.create.side_effect = ValueError("Agent name already exists")
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            with pytest.raises(ValueError, match="Agent name already exists"):
                await service.create(agent_create_data)
    
    @pytest.mark.asyncio
    async def test_agent_configuration_validation(self, mock_uow):
        """Test comprehensive agent configuration validation."""
        service = AgentService(mock_uow)
        
        # Test valid configuration
        valid_config = {
            "name": "Test Agent",
            "role": "Assistant",
            "goal": "Help users",
            "backstory": "I am helpful",
            "tools": ["web_search"],
            "llm": "gpt-4o-mini",
            "max_iter": 25,
            "allow_delegation": False,
            "verbose": True
        }
        
        service._validate_agent_configuration(valid_config)
        
        # Test invalid configuration (missing required fields)
        invalid_config = {
            "name": "Test Agent"
            # Missing role, goal, backstory
        }
        
        with pytest.raises(ValueError, match="Missing required field"):
            service._validate_agent_configuration(invalid_config)
    
    @pytest.mark.asyncio
    async def test_agent_activation_deactivation(self, mock_uow, mock_agent_repository):
        """Test agent activation and deactivation."""
        agent_id = uuid.uuid4()
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            # Test activation
            result = await service.activate(agent_id)
            assert result is not None
            
            # Test deactivation
            result = await service.deactivate(agent_id)
            assert result is not None
            
            # Verify repository calls
            assert mock_agent_repository.update.call_count == 2
    
    @pytest.mark.asyncio
    async def test_agent_export_import(self, mock_uow, mock_agent_repository):
        """Test agent export and import functionality."""
        agent_id = uuid.uuid4()
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            # Test export
            export_data = await service.export_agent(agent_id)
            
            assert export_data is not None
            assert "name" in export_data
            assert "role" in export_data
            assert "goal" in export_data
            assert "tools" in export_data
            
            # Test import
            imported_agent = await service.import_agent(export_data)
            
            assert imported_agent is not None
            mock_agent_repository.create.assert_called()
    
    @pytest.mark.asyncio
    async def test_agent_clone(self, mock_uow, mock_agent_repository):
        """Test agent cloning functionality."""
        agent_id = uuid.uuid4()
        new_name = "Cloned Agent"
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            cloned_agent = await service.clone_agent(agent_id, new_name)
            
            assert cloned_agent is not None
            mock_agent_repository.get.assert_called_once_with(agent_id)
            mock_agent_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_agent_metrics(self, mock_uow, mock_agent_repository):
        """Test agent metrics collection."""
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            metrics = await service.get_agent_metrics()
            
            assert "total_agents" in metrics
            assert "active_agents" in metrics
            assert "agents_by_role" in metrics
            assert metrics["total_agents"] >= 0
            assert metrics["active_agents"] >= 0
    
    @pytest.mark.asyncio
    async def test_agent_performance_tracking(self, mock_uow, mock_agent_repository):
        """Test agent performance tracking."""
        agent_id = uuid.uuid4()
        performance_data = {
            "execution_time": 120.5,
            "success_rate": 0.95,
            "task_count": 10
        }
        
        with patch("src.services.agent_service.AgentRepository", return_value=mock_agent_repository):
            service = AgentService(mock_uow)
            
            result = await service.update_performance_metrics(agent_id, performance_data)
            
            assert result is not None
            mock_agent_repository.update.assert_called_once()