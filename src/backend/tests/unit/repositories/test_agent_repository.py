"""
Unit tests for AgentRepository.

Tests the functionality of agent repository including
async and sync CRUD operations, custom queries, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from src.repositories.agent_repository import AgentRepository, SyncAgentRepository, get_sync_agent_repository
from src.models.agent import Agent


# Mock agent model
class MockAgent:
    def __init__(self, id="agent-123", name="Test Agent", role="Developer",
                 goal="Write code", backstory="Experienced developer", tools=None,
                 group_id="group-123", created_by_email="test@example.com",
                 created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.tools = tools or ["tool1", "tool2"]
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.delete = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.query.return_value = session
    session.filter.return_value = session
    return session


@pytest.fixture
def agent_repository(mock_async_session):
    """Create an agent repository with async session."""
    return AgentRepository(session=mock_async_session)


@pytest.fixture
def sync_agent_repository(mock_sync_session):
    """Create a sync agent repository."""
    return SyncAgentRepository(db=mock_sync_session)


@pytest.fixture
def sample_agents():
    """Create sample agents for testing."""
    return [
        MockAgent(id="agent-1", name="Agent 1", role="Developer"),
        MockAgent(id="agent-2", name="Agent 2", role="Tester"),
        MockAgent(id="agent-3", name="Agent 3", role="Designer")
    ]


class TestAgentRepositoryInit:
    """Test cases for AgentRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = AgentRepository(session=mock_async_session)
        
        assert repository.model == Agent
        assert repository.session == mock_async_session


class TestAgentRepositoryGet:
    """Test cases for get method."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, agent_repository, mock_async_session):
        """Test successful agent retrieval."""
        agent = MockAgent(id="agent-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.get("agent-123")
        
        assert result == agent
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Agent)))
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, agent_repository, mock_async_session):
        """Test get when agent not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.get("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_exception_handling(self, agent_repository, mock_async_session):
        """Test get with database exception."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await agent_repository.get("agent-123")
        
        mock_async_session.rollback.assert_called_once()


class TestAgentRepositoryUpdate:
    """Test cases for update method."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, agent_repository, mock_async_session):
        """Test successful agent update."""
        agent = MockAgent(id="agent-123", name="Old Name", group_id="group-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        update_data = {"name": "New Name", "role": "Senior Developer"}
        result = await agent_repository.update("agent-123", update_data)
        
        assert result == agent
        assert agent.name == "New Name"
        assert agent.role == "Senior Developer"
        mock_async_session.flush.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, agent_repository, mock_async_session):
        """Test update when agent not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        update_data = {"name": "New Name"}
        result = await agent_repository.update("nonexistent", update_data)
        
        assert result is None
        mock_async_session.flush.assert_not_called()
        mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_empty_data(self, agent_repository, mock_async_session):
        """Test update with empty data."""
        agent = MockAgent(id="agent-123", name="Original Name", group_id="group-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.update("agent-123", {})
        
        assert result == agent
        assert agent.name == "Original Name"  # Unchanged
        mock_async_session.flush.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_exception_handling(self, agent_repository, mock_async_session):
        """Test update with database exception."""
        agent = MockAgent(id="agent-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        mock_async_session.flush.side_effect = Exception("Flush error")
        
        with pytest.raises(Exception, match="Flush error"):
            await agent_repository.update("agent-123", {"name": "New Name"})
        
        mock_async_session.rollback.assert_called_once()


class TestAgentRepositoryDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, agent_repository, mock_async_session):
        """Test successful agent deletion."""
        agent = MockAgent(id="agent-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.delete("agent-123")
        
        assert result is True
        mock_async_session.delete.assert_called_once_with(agent)
        mock_async_session.flush.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, agent_repository, mock_async_session):
        """Test delete when agent not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.delete("nonexistent")
        
        assert result is False
        mock_async_session.delete.assert_not_called()
        mock_async_session.flush.assert_not_called()
        mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_exception_handling(self, agent_repository, mock_async_session):
        """Test delete with database exception."""
        agent = MockAgent(id="agent-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        mock_async_session.delete.side_effect = Exception("Delete error")
        
        with pytest.raises(Exception, match="Delete error"):
            await agent_repository.delete("agent-123")
        
        mock_async_session.rollback.assert_called_once()


class TestAgentRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, agent_repository, mock_async_session):
        """Test successful find by name."""
        agent = MockAgent(name="Specific Agent", group_id="group-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_by_name("Specific Agent")
        
        assert result == agent
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, agent_repository, mock_async_session):
        """Test find by name when agent doesn't exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_by_name("Nonexistent Agent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_multiple_returns_first(self, agent_repository, mock_async_session):
        """Test find by name returns first result when multiple exist."""
        agent1 = MockAgent(id="agent-1", name="Same Name", group_id="group-123")
        agent2 = MockAgent(id="agent-2", name="Same Name", group_id="group-123")
        mock_result = MockResult([agent1, agent2])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_by_name("Same Name")
        
        assert result == agent1
        mock_async_session.execute.assert_called_once()


class TestAgentRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, agent_repository, mock_async_session, sample_agents):
        """Test successful find all agents."""
        mock_result = MockResult(sample_agents)
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_all()
        
        assert len(result) == 3
        assert result == sample_agents
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(Agent)))
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, agent_repository, mock_async_session):
        """Test find all when no agents exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_returns_list(self, agent_repository, mock_async_session, sample_agents):
        """Test find all returns a list (not generator)."""
        mock_result = MockResult(sample_agents)
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_all()
        
        assert isinstance(result, list)
        assert len(result) == 3


class TestAgentRepositoryDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, agent_repository, mock_async_session):
        """Test successful delete all agents."""
        await agent_repository.delete_all()
        
        mock_async_session.execute.assert_called_once()
        mock_async_session.flush.assert_called_once()
        mock_async_session.commit.assert_called_once()
        
        # Verify the delete statement was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(delete(Agent)))


class TestSyncAgentRepositoryInit:
    """Test cases for SyncAgentRepository initialization."""
    
    def test_init_success(self, mock_sync_session):
        """Test successful initialization."""
        repository = SyncAgentRepository(db=mock_sync_session)
        
        assert repository.db == mock_sync_session


class TestSyncAgentRepositoryFindById:
    """Test cases for find_by_id method."""
    
    def test_find_by_id_success(self, sync_agent_repository, mock_sync_session):
        """Test successful find by ID."""
        agent = MockAgent(id=123)
        mock_sync_session.first.return_value = agent
        
        result = sync_agent_repository.find_by_id(123)
        
        assert result == agent
        mock_sync_session.query.assert_called_once_with(Agent)
        mock_sync_session.filter.assert_called_once()
        mock_sync_session.first.assert_called_once()
    
    def test_find_by_id_not_found(self, sync_agent_repository, mock_sync_session):
        """Test find by ID when agent not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_agent_repository.find_by_id(999)
        
        assert result is None
        mock_sync_session.query.assert_called_once_with(Agent)


class TestSyncAgentRepositoryFindByName:
    """Test cases for sync find_by_name method."""
    
    def test_find_by_name_success(self, sync_agent_repository, mock_sync_session):
        """Test successful sync find by name."""
        agent = MockAgent(name="Sync Agent", group_id="group-123")
        mock_sync_session.first.return_value = agent
        
        result = sync_agent_repository.find_by_name("Sync Agent")
        
        assert result == agent
        mock_sync_session.query.assert_called_once_with(Agent)
        mock_sync_session.filter.assert_called_once()
        mock_sync_session.first.assert_called_once()
    
    def test_find_by_name_not_found(self, sync_agent_repository, mock_sync_session):
        """Test sync find by name when agent not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_agent_repository.find_by_name("Nonexistent")
        
        assert result is None


class TestSyncAgentRepositoryFindAll:
    """Test cases for sync find_all method."""
    
    def test_find_all_success(self, sync_agent_repository, mock_sync_session, sample_agents):
        """Test successful sync find all."""
        mock_sync_session.all.return_value = sample_agents
        
        result = sync_agent_repository.find_all()
        
        assert result == sample_agents
        assert len(result) == 3
        mock_sync_session.query.assert_called_once_with(Agent)
        mock_sync_session.all.assert_called_once()
    
    def test_find_all_empty(self, sync_agent_repository, mock_sync_session):
        """Test sync find all when no agents exist."""
        mock_sync_session.all.return_value = []
        
        result = sync_agent_repository.find_all()
        
        assert result == []


class TestGetSyncAgentRepository:
    """Test cases for get_sync_agent_repository factory function."""
    
    def test_get_sync_agent_repository_success(self):
        """Test successful factory function call."""
        with patch('src.repositories.agent_repository.SessionLocal') as mock_session_local:
            mock_session = MagicMock()
            mock_session_local.return_value = mock_session
            
            result = get_sync_agent_repository()
            
            assert isinstance(result, SyncAgentRepository)
            assert result.db == mock_session
            mock_session_local.assert_called_once()


class TestAgentRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_get_then_update_flow(self, agent_repository, mock_async_session):
        """Test the flow from get to update."""
        agent = MockAgent(id="agent-123", name="Original Name", group_id="group-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        # The update method internally calls get
        result = await agent_repository.update("agent-123", {"name": "Updated Name"})
        
        assert result == agent
        assert agent.name == "Updated Name"
        assert mock_async_session.execute.call_count == 1  # One call for update's internal get
        mock_async_session.flush.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_then_delete_flow(self, agent_repository, mock_async_session):
        """Test the flow from get to delete."""
        agent = MockAgent(id="agent-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.delete("agent-123")
        
        assert result is True
        assert mock_async_session.execute.call_count == 1  # One call for delete's internal get
        mock_async_session.delete.assert_called_once_with(agent)
        mock_async_session.flush.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_with_actual_query_structure(self, agent_repository, mock_async_session):
        """Test find_by_name with verification of query structure."""
        agent = MockAgent(name="Query Test Agent", group_id="group-123")
        mock_result = MockResult([agent])
        mock_async_session.execute.return_value = mock_result
        
        result = await agent_repository.find_by_name("Query Test Agent")
        
        assert result == agent
        # Verify that execute was called with a select statement
        call_args = mock_async_session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # Basic check that it's a SQL query


class TestAgentRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_session_error(self, agent_repository, mock_async_session):
        """Test find by name when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await agent_repository.find_by_name("Error Agent")
    
    @pytest.mark.asyncio
    async def test_find_all_session_error(self, agent_repository, mock_async_session):
        """Test find all when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await agent_repository.find_all()
    
    @pytest.mark.asyncio
    async def test_delete_all_session_error(self, agent_repository, mock_async_session):
        """Test delete all when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await agent_repository.delete_all()
    
    def test_sync_find_by_id_session_error(self, sync_agent_repository, mock_sync_session):
        """Test sync find by ID when session raises an error."""
        mock_sync_session.query.side_effect = Exception("Sync session error")
        
        with pytest.raises(Exception, match="Sync session error"):
            sync_agent_repository.find_by_id(123)
    
    def test_sync_find_by_name_session_error(self, sync_agent_repository, mock_sync_session):
        """Test sync find by name when session raises an error."""
        mock_sync_session.query.side_effect = Exception("Sync session error")
        
        with pytest.raises(Exception, match="Sync session error"):
            sync_agent_repository.find_by_name("Error Agent")
    
    def test_sync_find_all_session_error(self, sync_agent_repository, mock_sync_session):
        """Test sync find all when session raises an error."""
        mock_sync_session.query.side_effect = Exception("Sync session error")
        
        with pytest.raises(Exception, match="Sync session error"):
            sync_agent_repository.find_all()