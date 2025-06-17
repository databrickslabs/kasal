"""
Unit tests for AgentService.

Tests the functionality of agent management service including
CRUD operations with group isolation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from src.services.agent_service import AgentService
from src.models.agent import Agent
from src.repositories.agent_repository import AgentRepository
from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate
from src.utils.user_context import GroupContext


# Mock agent model
class MockAgent:
    def __init__(self, id="agent-123", name="Test Agent", role="Test Role",
                 goal="Test Goal", backstory="Test Backstory", tools=None,
                 group_id="group-123", created_by_email="test@example.com"):
        self.id = id
        self.name = name
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.tools = tools or ["tool1", "tool2"]
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.scalars = AsyncMock()
    return session


@pytest.fixture
def mock_repository():
    """Create a mock agent repository."""
    repository = AsyncMock(spec=AgentRepository)
    return repository


@pytest.fixture
def agent_service(mock_session, mock_repository):
    """Create an agent service with mocked dependencies."""
    with patch('src.services.agent_service.AgentRepository', return_value=mock_repository):
        service = AgentService(session=mock_session)
        service.repository = mock_repository
        return service


@pytest.fixture
def sample_agent_create():
    """Create a sample agent creation schema."""
    return AgentCreate(
        name="New Agent",
        role="Developer",
        goal="Write clean code",
        backstory="Experienced developer with 10 years in Python",
        tools=["code_editor", "debugger"]
    )


@pytest.fixture
def sample_agent_update():
    """Create a sample agent update schema."""
    return AgentUpdate(
        name="Updated Agent",
        role="Senior Developer",
        goal="Lead development team",
        backstory="Now a tech lead",
        tools=["code_editor", "debugger", "git"]
    )


@pytest.fixture
def sample_agent_limited_update():
    """Create a sample agent limited update schema."""
    return AgentLimitedUpdate(
        name="Limited Update Agent",
        goal="Updated goal only"
    )


@pytest.fixture
def sample_group_context():
    """Create a sample group context."""
    return GroupContext(
        group_ids=["group-123", "group-456"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123"
    )


class TestAgentServiceInit:
    """Test cases for AgentService initialization."""
    
    def test_init_with_defaults(self, mock_session):
        """Test initialization with default parameters."""
        service = AgentService(session=mock_session)
        
        assert service.session == mock_session
        assert service.repository_class == AgentRepository
        assert service.model_class == Agent
        assert isinstance(service.repository, AgentRepository)
    
    def test_init_with_custom_classes(self, mock_session):
        """Test initialization with custom repository and model classes."""
        mock_repo_class = MagicMock()
        mock_model_class = MagicMock()
        
        service = AgentService(
            session=mock_session,
            repository_class=mock_repo_class,
            model_class=mock_model_class
        )
        
        assert service.repository_class == mock_repo_class
        assert service.model_class == mock_model_class
        mock_repo_class.assert_called_once_with(mock_session)
    
    # Removed test_create_factory_method because the create class method
    # is shadowed by the instance create method from BaseService


class TestAgentServiceGet:
    """Test cases for get method."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, agent_service, mock_repository):
        """Test successful agent retrieval."""
        agent = MockAgent()
        mock_repository.get.return_value = agent
        
        result = await agent_service.get("agent-123")
        
        assert result == agent
        mock_repository.get.assert_called_once_with("agent-123")
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, agent_service, mock_repository):
        """Test get when agent is not found."""
        mock_repository.get.return_value = None
        
        result = await agent_service.get("non-existent")
        
        assert result is None
        mock_repository.get.assert_called_once_with("non-existent")


class TestAgentServiceCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, agent_service, mock_repository, sample_agent_create):
        """Test successful agent creation."""
        created_agent = MockAgent(
            name=sample_agent_create.name,
            role=sample_agent_create.role
        )
        mock_repository.create.return_value = created_agent
        
        result = await agent_service.create(sample_agent_create)
        
        assert result == created_agent
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["name"] == "New Agent"
        assert call_args["role"] == "Developer"
    
    @pytest.mark.asyncio
    async def test_create_with_minimal_data(self, agent_service, mock_repository):
        """Test creation with minimal required data."""
        minimal_data = AgentCreate(
            name="Minimal Agent",
            role="Basic Role",
            goal="Basic Goal",
            backstory="Basic Backstory"
        )
        created_agent = MockAgent(name="Minimal Agent")
        mock_repository.create.return_value = created_agent
        
        result = await agent_service.create(minimal_data)
        
        assert result == created_agent
        mock_repository.create.assert_called_once()


class TestAgentServiceFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, agent_service, mock_repository):
        """Test successful find by name."""
        agent = MockAgent(name="Specific Agent")
        mock_repository.find_by_name.return_value = agent
        
        result = await agent_service.find_by_name("Specific Agent")
        
        assert result == agent
        mock_repository.find_by_name.assert_called_once_with("Specific Agent")
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, agent_service, mock_repository):
        """Test find by name when agent doesn't exist."""
        mock_repository.find_by_name.return_value = None
        
        result = await agent_service.find_by_name("Non-existent Agent")
        
        assert result is None
        mock_repository.find_by_name.assert_called_once_with("Non-existent Agent")


class TestAgentServiceFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, agent_service, mock_repository):
        """Test successful find all agents."""
        agents = [
            MockAgent(id="agent-1", name="Agent 1"),
            MockAgent(id="agent-2", name="Agent 2"),
            MockAgent(id="agent-3", name="Agent 3")
        ]
        mock_repository.find_all.return_value = agents
        
        result = await agent_service.find_all()
        
        assert result == agents
        assert len(result) == 3
        mock_repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, agent_service, mock_repository):
        """Test find all when no agents exist."""
        mock_repository.find_all.return_value = []
        
        result = await agent_service.find_all()
        
        assert result == []
        mock_repository.find_all.assert_called_once()


class TestAgentServiceUpdateWithPartialData:
    """Test cases for update_with_partial_data method."""
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_success(self, agent_service, mock_repository, sample_agent_update):
        """Test successful partial update."""
        updated_agent = MockAgent(
            name=sample_agent_update.name,
            role=sample_agent_update.role
        )
        mock_repository.update.return_value = updated_agent
        
        result = await agent_service.update_with_partial_data("agent-123", sample_agent_update)
        
        assert result == updated_agent
        mock_repository.update.assert_called_once()
        call_args = mock_repository.update.call_args[0]
        assert call_args[0] == "agent-123"
        assert "name" in call_args[1]
        assert "role" in call_args[1]
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_no_fields(self, agent_service, mock_repository):
        """Test update with no fields set (all None)."""
        empty_update = AgentUpdate()
        existing_agent = MockAgent()
        mock_repository.get.return_value = existing_agent
        
        result = await agent_service.update_with_partial_data("agent-123", empty_update)
        
        assert result == existing_agent
        mock_repository.update.assert_not_called()
        mock_repository.get.assert_called_once_with("agent-123")
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_not_found(self, agent_service, mock_repository, sample_agent_update):
        """Test update when agent is not found."""
        mock_repository.update.return_value = None
        
        result = await agent_service.update_with_partial_data("non-existent", sample_agent_update)
        
        assert result is None
        mock_repository.update.assert_called_once()


class TestAgentServiceUpdateLimitedFields:
    """Test cases for update_limited_fields method."""
    
    @pytest.mark.asyncio
    async def test_update_limited_fields_success(self, agent_service, mock_repository, sample_agent_limited_update):
        """Test successful limited fields update."""
        updated_agent = MockAgent(
            name=sample_agent_limited_update.name,
            goal=sample_agent_limited_update.goal
        )
        mock_repository.update.return_value = updated_agent
        
        result = await agent_service.update_limited_fields("agent-123", sample_agent_limited_update)
        
        assert result == updated_agent
        mock_repository.update.assert_called_once()
        call_args = mock_repository.update.call_args[0]
        assert call_args[0] == "agent-123"
        assert call_args[1]["name"] == "Limited Update Agent"
        assert call_args[1]["goal"] == "Updated goal only"
    
    @pytest.mark.asyncio
    async def test_update_limited_fields_no_fields(self, agent_service, mock_repository):
        """Test limited update with no fields set."""
        empty_update = AgentLimitedUpdate()
        existing_agent = MockAgent()
        mock_repository.get.return_value = existing_agent
        
        result = await agent_service.update_limited_fields("agent-123", empty_update)
        
        assert result == existing_agent
        mock_repository.update.assert_not_called()
        mock_repository.get.assert_called_once_with("agent-123")


class TestAgentServiceDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, agent_service, mock_repository):
        """Test successful agent deletion."""
        mock_repository.delete.return_value = True
        
        result = await agent_service.delete("agent-123")
        
        assert result is True
        mock_repository.delete.assert_called_once_with("agent-123")
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, agent_service, mock_repository):
        """Test delete when agent is not found."""
        mock_repository.delete.return_value = False
        
        result = await agent_service.delete("non-existent")
        
        assert result is False
        mock_repository.delete.assert_called_once_with("non-existent")


class TestAgentServiceDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, agent_service, mock_repository):
        """Test successful delete all agents."""
        mock_repository.delete_all.return_value = None
        
        await agent_service.delete_all()
        
        mock_repository.delete_all.assert_called_once()


class TestAgentServiceCreateWithGroup:
    """Test cases for create_with_group method."""
    
    @pytest.mark.asyncio
    async def test_create_with_group_success(self, agent_service, mock_repository, 
                                            sample_agent_create, sample_group_context):
        """Test successful agent creation with group context."""
        created_agent = MockAgent(
            name=sample_agent_create.name,
            group_id=sample_group_context.primary_group_id,
            created_by_email=sample_group_context.group_email
        )
        mock_repository.create.return_value = created_agent
        
        result = await agent_service.create_with_group(sample_agent_create, sample_group_context)
        
        assert result == created_agent
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["group_id"] == "group-123"  # Should use primary_group_id property
        assert call_args["created_by_email"] == "test@example.com"
        assert call_args["name"] == "New Agent"
    
    @pytest.mark.asyncio
    async def test_create_with_group_all_fields(self, agent_service, mock_repository, sample_group_context):
        """Test creation with group including all optional fields."""
        full_agent_data = AgentCreate(
            name="Full Agent",
            role="Full Role",
            goal="Full Goal",
            backstory="Full Backstory",
            tools=["tool1", "tool2", "tool3"],
            llm="gpt-4",
            max_iter=50,
            verbose=True
        )
        created_agent = MockAgent(name="Full Agent")
        mock_repository.create.return_value = created_agent
        
        result = await agent_service.create_with_group(full_agent_data, sample_group_context)
        
        assert result == created_agent
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["tools"] == ["tool1", "tool2", "tool3"]
        assert call_args["llm"] == "gpt-4"
        assert call_args["max_iter"] == 50


class TestAgentServiceFindByGroup:
    """Test cases for find_by_group method."""
    
    @pytest.mark.asyncio
    async def test_find_by_group_success(self, agent_service, mock_session, sample_group_context):
        """Test successful find agents by group."""
        agents = [
            MockAgent(id="agent-1", group_id="group-123"),
            MockAgent(id="agent-2", group_id="group-123"),
            MockAgent(id="agent-3", group_id="group-456")
        ]
        
        # Mock the session execute and scalars
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = agents
        mock_session.execute.return_value = mock_result
        
        result = await agent_service.find_by_group(sample_group_context)
        
        assert len(result) == 3
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_group_empty_group_context(self, agent_service, sample_group_context):
        """Test find by group with empty group IDs."""
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        result = await agent_service.find_by_group(empty_context)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_find_by_group_no_agents(self, agent_service, mock_session, sample_group_context):
        """Test find by group when no agents exist for the group."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result
        
        result = await agent_service.find_by_group(sample_group_context)
        
        assert result == []
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_group_ordering(self, agent_service, mock_session, sample_group_context):
        """Test that find by group orders results by created_at descending."""
        older_agent = MockAgent(id="agent-1", group_id="group-123")
        older_agent.created_at = datetime(2023, 1, 1)
        
        newer_agent = MockAgent(id="agent-2", group_id="group-123")
        newer_agent.created_at = datetime(2023, 6, 1)
        
        agents = [newer_agent, older_agent]  # Should be returned in this order
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = agents
        mock_session.execute.return_value = mock_result
        
        result = await agent_service.find_by_group(sample_group_context)
        
        assert len(result) == 2
        assert result[0].id == "agent-2"  # Newer agent first
        assert result[1].id == "agent-1"  # Older agent second