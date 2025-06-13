"""
Unit tests for AgentsRouter.

Tests the functionality of the agents router including
agent CRUD operations, group/tenant context handling, and error scenarios.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate


@pytest.fixture
def mock_agent_service():
    """Create a mock agent service."""
    service = AsyncMock()
    
    # Create mock agent objects
    mock_agent = MagicMock()
    mock_agent.id = str(uuid.uuid4())
    mock_agent.name = "Test Agent"
    mock_agent.description = "A test agent"
    mock_agent.role = "researcher"
    mock_agent.goal = "Research topics"
    mock_agent.backstory = "Expert researcher"
    mock_agent.tools = []
    mock_agent.llm_config = {}
    mock_agent.is_active = True
    
    # Setup service method returns
    service.create_with_group.return_value = mock_agent
    service.find_by_group.return_value = [mock_agent]
    service.get.return_value = mock_agent
    service.update_with_partial_data.return_value = mock_agent
    service.update_limited_fields.return_value = mock_agent
    service.delete.return_value = True
    service.delete_all.return_value = None
    
    return service


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.is_valid.return_value = True
    context.group_id = str(uuid.uuid4())
    return context




@pytest.fixture
def agent_create_data():
    """Create test data for agent creation."""
    return AgentCreate(
        name="Test Agent",
        description="A test agent for testing",
        role="researcher",
        goal="Research and analyze topics",
        backstory="An expert in research and analysis",
        tools=[],
        llm_config={}
    )


@pytest.fixture
def agent_update_data():
    """Create test data for agent updates."""
    return AgentUpdate(
        name="Updated Agent",
        description="Updated test agent",
        role="analyst",
        goal="Analyze data",
        backstory="Updated backstory",
        tools=[],
        llm_config={}
    )


@pytest.fixture
def agent_limited_update_data():
    """Create test data for limited agent updates."""
    return AgentLimitedUpdate(
        name="Limited Update Agent",
        description="Limited update description"
    )


class TestAgentsRouter:
    """Test cases for AgentsRouter."""
    
    @pytest.mark.asyncio
    async def test_create_agent_with_group_context(self, mock_agent_service, mock_group_context, agent_create_data):
        """Test successful agent creation with group context."""
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import create_agent
            
            result = await create_agent(
                agent_create_data,
                service=mock_agent_service,
                group_context=mock_group_context
            )
            
            assert result is not None
            assert result.name == "Test Agent"
            mock_agent_service.create_with_group.assert_called_once_with(agent_create_data, mock_group_context)
    
    
    @pytest.mark.asyncio
    async def test_create_agent_no_valid_context(self, mock_agent_service, agent_create_data):
        """Test agent creation with no valid context."""
        invalid_group_context = MagicMock()
        invalid_group_context.is_valid.return_value = False
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import create_agent
            
            with pytest.raises(HTTPException) as exc_info:
                await create_agent(
                    agent_create_data,
                    service=mock_agent_service,
                    group_context=invalid_group_context
                )
            
            assert exc_info.value.status_code == 400
            assert "No valid group context provided" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_list_agents_with_group_context(self, mock_agent_service, mock_group_context):
        """Test successful agent listing with group context."""
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import list_agents
            
            result = await list_agents(
                service=mock_agent_service,
                group_context=mock_group_context
            )
            
            assert len(result) == 1
            assert result[0].name == "Test Agent"
            mock_agent_service.find_by_group.assert_called_once_with(mock_group_context)
    
    
    @pytest.mark.asyncio
    async def test_list_agents_no_valid_context(self, mock_agent_service):
        """Test agent listing with no valid context returns empty list."""
        invalid_group_context = MagicMock()
        invalid_group_context.is_valid.return_value = False
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import list_agents
            
            result = await list_agents(
                service=mock_agent_service,
                group_context=invalid_group_context
            )
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_agent_success(self, mock_agent_service):
        """Test successful agent retrieval."""
        agent_id = str(uuid.uuid4())
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import get_agent
            
            result = await get_agent(agent_id, service=mock_agent_service)
            
            assert result is not None
            assert result.name == "Test Agent"
            mock_agent_service.get.assert_called_once_with(agent_id)
    
    @pytest.mark.asyncio
    async def test_get_agent_not_found(self, mock_agent_service):
        """Test getting a non-existent agent."""
        agent_id = str(uuid.uuid4())
        mock_agent_service.get.return_value = None
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import get_agent
            
            with pytest.raises(HTTPException) as exc_info:
                await get_agent(agent_id, service=mock_agent_service)
            
            assert exc_info.value.status_code == 404
            assert "Agent not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_update_agent_full_success(self, mock_agent_service, agent_update_data):
        """Test successful full agent update."""
        agent_id = str(uuid.uuid4())
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import update_agent_full
            
            result = await update_agent_full(
                agent_id,
                agent_update_data,
                service=mock_agent_service
            )
            
            assert result is not None
            mock_agent_service.update_with_partial_data.assert_called_once_with(agent_id, agent_update_data)
    
    @pytest.mark.asyncio
    async def test_update_agent_full_not_found(self, mock_agent_service, agent_update_data):
        """Test full update of non-existent agent."""
        agent_id = str(uuid.uuid4())
        mock_agent_service.update_with_partial_data.return_value = None
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import update_agent_full
            
            with pytest.raises(HTTPException) as exc_info:
                await update_agent_full(
                    agent_id,
                    agent_update_data,
                    service=mock_agent_service
                )
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_update_agent_limited_success(self, mock_agent_service, agent_limited_update_data):
        """Test successful limited agent update."""
        agent_id = str(uuid.uuid4())
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import update_agent
            
            result = await update_agent(
                agent_id,
                agent_limited_update_data,
                service=mock_agent_service
            )
            
            assert result is not None
            mock_agent_service.update_limited_fields.assert_called_once_with(agent_id, agent_limited_update_data)
    
    @pytest.mark.asyncio
    async def test_update_agent_limited_not_found(self, mock_agent_service, agent_limited_update_data):
        """Test limited update of non-existent agent."""
        agent_id = str(uuid.uuid4())
        mock_agent_service.update_limited_fields.return_value = None
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import update_agent
            
            with pytest.raises(HTTPException) as exc_info:
                await update_agent(
                    agent_id,
                    agent_limited_update_data,
                    service=mock_agent_service
                )
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_agent_success(self, mock_agent_service):
        """Test successful agent deletion."""
        agent_id = str(uuid.uuid4())
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import delete_agent
            
            await delete_agent(agent_id, service=mock_agent_service)
            
            mock_agent_service.delete.assert_called_once_with(agent_id)
    
    @pytest.mark.asyncio
    async def test_delete_agent_not_found(self, mock_agent_service):
        """Test deleting a non-existent agent."""
        agent_id = str(uuid.uuid4())
        mock_agent_service.delete.return_value = False
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import delete_agent
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_agent(agent_id, service=mock_agent_service)
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_delete_all_agents_success(self, mock_agent_service):
        """Test successful deletion of all agents."""
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import delete_all_agents
            
            await delete_all_agents(service=mock_agent_service)
            
            mock_agent_service.delete_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_agents_integrity_error(self, mock_agent_service):
        """Test deletion of all agents with integrity constraint violation."""
        mock_agent_service.delete_all.side_effect = IntegrityError("stmt", "params", "orig")
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import delete_all_agents
            
            with pytest.raises(HTTPException) as exc_info:
                await delete_all_agents(service=mock_agent_service)
            
            assert exc_info.value.status_code == 409
            assert "Cannot delete agents because some are still referenced by tasks" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_create_agent_generic_error(self, mock_agent_service, mock_group_context, agent_create_data):
        """Test agent creation with generic error."""
        mock_agent_service.create_with_group.side_effect = Exception("Database error")
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import create_agent
            
            with pytest.raises(HTTPException) as exc_info:
                await create_agent(
                    agent_create_data,
                    service=mock_agent_service,
                    group_context=mock_group_context
                )
            
            assert exc_info.value.status_code == 500
    
    @pytest.mark.asyncio
    async def test_list_agents_generic_error(self, mock_agent_service, mock_group_context):
        """Test agent listing with generic error."""
        mock_agent_service.find_by_group.side_effect = Exception("Database error")
        
        with patch("src.api.agents_router.get_agent_service", return_value=mock_agent_service):
            from src.api.agents_router import list_agents
            
            with pytest.raises(HTTPException) as exc_info:
                await list_agents(
                    service=mock_agent_service,
                    group_context=mock_group_context
                )
            
            assert exc_info.value.status_code == 500