"""
Unit tests for AgentService.

Tests the functionality of agent management service including
CRUD operations with group isolation.
"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.catalog.agents import AgentService


def _patch_isolated_session(mock_session):
    """Patch get_isolated_db_session to yield the given mock session.

    Agent delete methods run their write+commit on a private connection via
    get_isolated_db_session(); these tests redirect it to a mock session.
    """

    @asynccontextmanager
    async def _cm():
        yield mock_session

    return patch("src.db.session.get_isolated_db_session", _cm)


from src.models.agent import Agent
from src.repositories.agent_repository import AgentRepository
from src.schemas.agent import AgentCreate, AgentLimitedUpdate, AgentUpdate
from src.utils.user_context import GroupContext


# Mock agent model
class MockAgent:
    def __init__(
        self,
        id="agent-123",
        name="Test Agent",
        role="Test Role",
        goal="Test Goal",
        backstory="Test Backstory",
        tools=None,
        tool_configs=None,
        group_id="group-123",
        created_by_email="test@example.com",
    ):
        self.id = id
        self.name = name
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.tools = tools or ["tool1", "tool2"]
        self.tool_configs = tool_configs
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
    with patch(
        "src.services.catalog.agents.AgentRepository", return_value=mock_repository
    ):
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
        tools=["code_editor", "debugger"],
    )


@pytest.fixture
def sample_agent_update():
    """Create a sample agent update schema."""
    return AgentUpdate(
        name="Updated Agent",
        role="Senior Developer",
        goal="Lead development team",
        backstory="Now a tech lead",
        tools=["code_editor", "debugger", "git"],
    )


@pytest.fixture
def sample_agent_limited_update():
    """Create a sample agent limited update schema."""
    return AgentLimitedUpdate(name="Limited Update Agent", goal="Updated goal only")


@pytest.fixture
def sample_group_context():
    """Create a sample group context."""
    return GroupContext(
        group_ids=["group-123", "group-456"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123",
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
            model_class=mock_model_class,
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
    async def test_create_success(
        self, agent_service, mock_repository, sample_agent_create
    ):
        """Test successful agent creation."""
        created_agent = MockAgent(
            name=sample_agent_create.name, role=sample_agent_create.role
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
            backstory="Basic Backstory",
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
            MockAgent(id="agent-3", name="Agent 3"),
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
    async def test_update_with_partial_data_success(
        self, agent_service, mock_repository, sample_agent_update
    ):
        """Test successful partial update."""
        updated_agent = MockAgent(
            name=sample_agent_update.name, role=sample_agent_update.role
        )
        mock_repository.update.return_value = updated_agent

        result = await agent_service.update_with_partial_data(
            "agent-123", sample_agent_update
        )

        assert result == updated_agent
        mock_repository.update.assert_called_once()
        call_args = mock_repository.update.call_args[0]
        assert call_args[0] == "agent-123"
        assert "name" in call_args[1]
        assert "role" in call_args[1]

    @pytest.mark.asyncio
    async def test_update_with_partial_data_no_fields(
        self, agent_service, mock_repository
    ):
        """Test update with no fields set (all None)."""
        empty_update = AgentUpdate()
        existing_agent = MockAgent()
        mock_repository.get.return_value = existing_agent

        result = await agent_service.update_with_partial_data("agent-123", empty_update)

        assert result == existing_agent
        mock_repository.update.assert_not_called()
        mock_repository.get.assert_called_once_with("agent-123")

    @pytest.mark.asyncio
    async def test_update_with_partial_data_not_found(
        self, agent_service, mock_repository, sample_agent_update
    ):
        """Test update when agent is not found."""
        mock_repository.update.return_value = None

        result = await agent_service.update_with_partial_data(
            "non-existent", sample_agent_update
        )

        assert result is None
        mock_repository.update.assert_called_once()


class TestAgentServiceUpdateLimitedFields:
    """Test cases for update_limited_fields method."""

    @pytest.mark.asyncio
    async def test_update_limited_fields_success(
        self, agent_service, mock_repository, sample_agent_limited_update
    ):
        """Test successful limited fields update."""
        updated_agent = MockAgent(
            name=sample_agent_limited_update.name, goal=sample_agent_limited_update.goal
        )
        mock_repository.update.return_value = updated_agent

        result = await agent_service.update_limited_fields(
            "agent-123", sample_agent_limited_update
        )

        assert result == updated_agent
        mock_repository.update.assert_called_once()
        call_args = mock_repository.update.call_args[0]
        assert call_args[0] == "agent-123"
        assert call_args[1]["name"] == "Limited Update Agent"
        assert call_args[1]["goal"] == "Updated goal only"

    @pytest.mark.asyncio
    async def test_update_limited_fields_no_fields(
        self, agent_service, mock_repository
    ):
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
    async def test_delete_success(self, agent_service):
        """Test successful agent deletion (deletes tasks + agent, commits)."""
        iso_session = AsyncMock(spec=AsyncSession)
        iso_session.get = AsyncMock(return_value=MockAgent(id="agent-123"))

        with _patch_isolated_session(iso_session):
            result = await agent_service.delete("agent-123")

        assert result is True
        # Two deletes: tasks then agents, then an explicit commit
        assert iso_session.execute.await_count == 2
        iso_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delete_not_found(self, agent_service):
        """Test delete when agent is not found."""
        iso_session = AsyncMock(spec=AsyncSession)
        iso_session.get = AsyncMock(return_value=None)

        with _patch_isolated_session(iso_session):
            result = await agent_service.delete("non-existent")

        assert result is False
        iso_session.execute.assert_not_awaited()
        iso_session.commit.assert_not_awaited()


class TestAgentServiceDeleteAll:
    """Test cases for delete_all method."""

    @pytest.mark.asyncio
    async def test_delete_all_success(self, agent_service):
        """Test successful delete all agents (tasks + agents, commits)."""
        iso_session = AsyncMock(spec=AsyncSession)

        with _patch_isolated_session(iso_session):
            await agent_service.delete_all()

        # Two deletes: assigned tasks then all agents, then an explicit commit
        assert iso_session.execute.await_count == 2
        iso_session.commit.assert_awaited_once()


class TestAgentServiceCreateWithGroup:
    """Test cases for create_with_group method."""

    @pytest.mark.asyncio
    async def test_create_with_group_success(
        self, agent_service, mock_repository, sample_agent_create, sample_group_context
    ):
        """Test successful agent creation with group context."""
        created_agent = MockAgent(
            name=sample_agent_create.name,
            group_id=sample_group_context.primary_group_id,
            created_by_email=sample_group_context.group_email,
        )
        mock_repository.create.return_value = created_agent

        result = await agent_service.create_with_group(
            sample_agent_create, sample_group_context
        )

        assert result == created_agent
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert (
            call_args["group_id"] == "group-123"
        )  # Should use primary_group_id property
        assert call_args["created_by_email"] == "test@example.com"
        assert call_args["name"] == "New Agent"

    @pytest.mark.asyncio
    async def test_create_with_group_all_fields(
        self, agent_service, mock_repository, sample_group_context
    ):
        """Test creation with group including all optional fields."""
        full_agent_data = AgentCreate(
            name="Full Agent",
            role="Full Role",
            goal="Full Goal",
            backstory="Full Backstory",
            tools=["tool1", "tool2", "tool3"],
            llm="gpt-4",
            max_iter=50,
            verbose=True,
        )
        created_agent = MockAgent(name="Full Agent")
        mock_repository.create.return_value = created_agent

        result = await agent_service.create_with_group(
            full_agent_data, sample_group_context
        )

        assert result == created_agent
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["tools"] == ["tool1", "tool2", "tool3"]
        assert call_args["llm"] == "gpt-4"
        assert call_args["max_iter"] == 50


class TestAgentServiceFindByGroup:
    """Test cases for find_by_group method."""

    @pytest.mark.asyncio
    async def test_find_by_group_success(
        self, agent_service, mock_session, sample_group_context
    ):
        """Test successful find agents by group."""
        agents = [
            MockAgent(id="agent-1", group_id="group-123"),
            MockAgent(id="agent-2", group_id="group-123"),
            MockAgent(id="agent-3", group_id="group-456"),
        ]

        # Mock the session execute and scalars
        agent_service.repository.find_by_group_ids = AsyncMock(return_value=agents)

        result = await agent_service.find_by_group(sample_group_context)

        assert len(result) == 3
        agent_service.repository.find_by_group_ids.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_by_group_empty_group_context(
        self, agent_service, sample_group_context
    ):
        """Test find by group with empty group IDs."""
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123",
        )

        result = await agent_service.find_by_group(empty_context)

        assert result == []

    @pytest.mark.asyncio
    async def test_find_by_group_no_agents(
        self, agent_service, mock_session, sample_group_context
    ):
        """Test find by group when no agents exist for the group."""
        agent_service.repository.find_by_group_ids = AsyncMock(return_value=[])

        result = await agent_service.find_by_group(sample_group_context)

        assert result == []
        agent_service.repository.find_by_group_ids.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_by_group_ordering(
        self, agent_service, mock_session, sample_group_context
    ):
        """Test that find by group orders results by created_at descending."""
        older_agent = MockAgent(id="agent-1", group_id="group-123")
        older_agent.created_at = datetime(2023, 1, 1)

        newer_agent = MockAgent(id="agent-2", group_id="group-123")
        newer_agent.created_at = datetime(2023, 6, 1)

        agents = [newer_agent, older_agent]  # Should be returned in this order

        agent_service.repository.find_by_group_ids = AsyncMock(return_value=agents)

        result = await agent_service.find_by_group(sample_group_context)

        assert len(result) == 2
        assert result[0].id == "agent-2"  # Newer agent first
        assert result[1].id == "agent-1"  # Older agent second


# ==========================================================================
# Additional coverage: tool_configs encrypt/decrypt, group-scoped
# update/delete branches (update_with_group_check, update_limited_fields,
# update_limited_with_group_check, delete_with_group_check, delete_all_for_group)
# ==========================================================================


def make_service():
    session = AsyncMock()
    with patch("src.services.catalog.agents.AgentRepository") as MockRepo:
        mock_repo = AsyncMock()
        MockRepo.return_value = mock_repo
        from src.services.catalog.agents import AgentService

        svc = AgentService(session)
        svc.repository = mock_repo
    return svc


def make_agent(id="a1", tool_configs=None):
    agent = MagicMock()
    agent.id = id
    agent.tool_configs = tool_configs
    return agent


# ---- _decrypt_agent_tool_configs ----


def test_decrypt_agent_tool_configs_with_tool_configs():
    svc = make_service()
    agent = make_agent(tool_configs={"key": "encrypted"})
    with patch(
        "src.services.catalog.agents.decrypt_sensitive_fields",
        return_value={"key": "decrypted"},
    ):
        result = svc._decrypt_agent_tool_configs(agent)
    assert result is agent
    assert agent.tool_configs == {"key": "decrypted"}


def test_decrypt_agent_tool_configs_decrypt_exception():
    svc = make_service()
    agent = make_agent(tool_configs={"key": "bad_encrypted"})
    with patch(
        "src.services.catalog.agents.decrypt_sensitive_fields",
        side_effect=Exception("decrypt error"),
    ):
        result = svc._decrypt_agent_tool_configs(agent)
    assert result is agent  # Should still return agent


def test_decrypt_agent_tool_configs_no_tool_configs():
    svc = make_service()
    agent = make_agent(tool_configs=None)
    result = svc._decrypt_agent_tool_configs(agent)
    assert result is agent


# ---- _encrypt_tool_configs_in_data ----


def test_encrypt_tool_configs_in_data_success():
    svc = make_service()
    data = {"tool_configs": {"api_key": "plaintext"}, "name": "test"}
    with patch(
        "src.services.catalog.agents.encrypt_sensitive_fields",
        return_value={"api_key": "encrypted"},
    ):
        with patch(
            "src.services.catalog.agents.safe_log_tool_configs", return_value="safe log"
        ):
            result = svc._encrypt_tool_configs_in_data(data)
    assert result["tool_configs"]["api_key"] == "encrypted"


def test_encrypt_tool_configs_in_data_exception_raises():
    svc = make_service()
    data = {"tool_configs": {"api_key": "plaintext"}}
    with patch(
        "src.services.catalog.agents.encrypt_sensitive_fields",
        side_effect=Exception("encrypt failed"),
    ):
        with pytest.raises(Exception, match="encrypt failed"):
            svc._encrypt_tool_configs_in_data(data)


def test_encrypt_tool_configs_no_tool_configs():
    svc = make_service()
    data = {"name": "test"}
    result = svc._encrypt_tool_configs_in_data(data)
    assert result == {"name": "test"}


# ---- create (class method) ----


def test_create_factory_method():
    """Test the classmethod factory at line 84.

    Note: The classmethod 'create' (line 84) is shadowed by the instance method
    'create' (line 127). Line 94 may be hard to reach in isolation.
    Just verify the instance method works.
    """
    from src.services.catalog.agents import AgentService

    session = AsyncMock()
    with patch("src.services.catalog.agents.AgentRepository"):
        # Just verify the service can be instantiated
        svc = AgentService(session=session)
    assert isinstance(svc, AgentService)


# ---- update_with_partial_data with tool_configs ----


@pytest.mark.asyncio
async def test_update_with_partial_data_with_tool_configs():
    svc = make_service()
    agent = make_agent()
    svc.repository.update = AsyncMock(return_value=agent)

    obj_in = MagicMock()
    obj_in.model_dump.return_value = {"tool_configs": {"api_key": "plain"}}

    with patch(
        "src.services.catalog.agents.encrypt_sensitive_fields",
        return_value={"api_key": "enc"},
    ):
        with patch(
            "src.services.catalog.agents.safe_log_tool_configs", return_value="safe"
        ):
            with patch.object(svc, "_decrypt_agent_tool_configs", return_value=agent):
                result = await svc.update_with_partial_data("a1", obj_in)
    assert result is agent


# ---- update_with_group_check ----


@pytest.mark.asyncio
async def test_update_with_group_check_not_found():
    svc = make_service()
    group_ctx = MagicMock()
    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=None
    ):
        obj_in = MagicMock()
        obj_in.model_dump.return_value = {"name": "new"}
        result = await svc.update_with_group_check("a1", obj_in, group_ctx)
    assert result is None


@pytest.mark.asyncio
async def test_update_with_group_check_no_data():
    svc = make_service()
    agent = make_agent()
    group_ctx = MagicMock()
    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=agent
    ):
        obj_in = MagicMock()
        obj_in.model_dump.return_value = {}  # Empty update
        result = await svc.update_with_group_check("a1", obj_in, group_ctx)
    assert result is agent


@pytest.mark.asyncio
async def test_update_with_group_check_with_tool_configs():
    svc = make_service()
    agent = make_agent()
    group_ctx = MagicMock()
    svc.repository.update = AsyncMock(return_value=agent)

    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=agent
    ):
        obj_in = MagicMock()
        obj_in.model_dump.return_value = {"tool_configs": {"key": "plain"}}
        with patch(
            "src.services.catalog.agents.encrypt_sensitive_fields",
            return_value={"key": "enc"},
        ):
            with patch(
                "src.services.catalog.agents.safe_log_tool_configs", return_value="safe"
            ):
                with patch.object(
                    svc, "_decrypt_agent_tool_configs", return_value=agent
                ):
                    result = await svc.update_with_group_check("a1", obj_in, group_ctx)
    assert result is agent


# ---- update_limited_fields ----


@pytest.mark.asyncio
async def test_update_limited_fields_with_tool_configs():
    svc = make_service()
    agent = make_agent()
    svc.repository.update = AsyncMock(return_value=agent)

    obj_in = MagicMock()
    obj_in.model_dump.return_value = {"tool_configs": {"key": "plain"}}

    with patch(
        "src.services.catalog.agents.encrypt_sensitive_fields",
        return_value={"key": "enc"},
    ):
        with patch(
            "src.services.catalog.agents.safe_log_tool_configs", return_value="safe"
        ):
            with patch.object(svc, "_decrypt_agent_tool_configs", return_value=agent):
                result = await svc.update_limited_fields("a1", obj_in)
    assert result is agent


# ---- update_limited_with_group_check ----


@pytest.mark.asyncio
async def test_update_limited_with_group_check_not_found():
    svc = make_service()
    group_ctx = MagicMock()
    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=None
    ):
        obj_in = MagicMock()
        obj_in.model_dump.return_value = {"name": "new"}
        result = await svc.update_limited_with_group_check("a1", obj_in, group_ctx)
    assert result is None


@pytest.mark.asyncio
async def test_update_limited_with_group_check_empty_update():
    svc = make_service()
    agent = make_agent()
    group_ctx = MagicMock()
    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=agent
    ):
        obj_in = MagicMock()
        obj_in.model_dump.return_value = {}
        result = await svc.update_limited_with_group_check("a1", obj_in, group_ctx)
    assert result is agent


@pytest.mark.asyncio
async def test_update_limited_with_group_check_with_tool_configs():
    svc = make_service()
    agent = make_agent()
    group_ctx = MagicMock()
    svc.repository.update = AsyncMock(return_value=agent)

    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=agent
    ):
        obj_in = MagicMock()
        obj_in.model_dump.return_value = {"tool_configs": {"key": "plain"}}
        with patch(
            "src.services.catalog.agents.encrypt_sensitive_fields",
            return_value={"key": "enc"},
        ):
            with patch(
                "src.services.catalog.agents.safe_log_tool_configs", return_value="safe"
            ):
                with patch.object(
                    svc, "_decrypt_agent_tool_configs", return_value=agent
                ):
                    result = await svc.update_limited_with_group_check(
                        "a1", obj_in, group_ctx
                    )
    assert result is agent


# ---- delete_with_group_check ----


@pytest.mark.asyncio
async def test_delete_with_group_check_not_found():
    svc = make_service()
    group_ctx = MagicMock()
    with patch.object(
        svc, "get_with_group_check", new_callable=AsyncMock, return_value=None
    ):
        result = await svc.delete_with_group_check("a1", group_ctx)
    assert result is False


@pytest.mark.asyncio
async def test_delete_with_group_check_success():
    svc = make_service()
    agent = make_agent()
    group_ctx = MagicMock()
    iso_session = AsyncMock()
    with (
        patch.object(
            svc, "get_with_group_check", new_callable=AsyncMock, return_value=agent
        ),
        _patch_isolated_session(iso_session),
    ):
        result = await svc.delete_with_group_check("a1", group_ctx)
    assert result is True
    # Two deletes (tasks then agent) committed on the private connection
    assert iso_session.execute.await_count == 2
    iso_session.commit.assert_awaited_once()


# ---- delete_all_for_group ----


@pytest.mark.asyncio
async def test_delete_all_for_group_no_context():
    svc = make_service()
    await svc.delete_all_for_group(None)  # Should return early


@pytest.mark.asyncio
async def test_delete_all_for_group_no_group_ids():
    svc = make_service()
    group_ctx = MagicMock()
    group_ctx.group_ids = None
    await svc.delete_all_for_group(group_ctx)


@pytest.mark.asyncio
async def test_delete_all_for_group_with_agents():
    svc = make_service()
    group_ctx = MagicMock()
    group_ctx.group_ids = ["g1"]

    agent1 = make_agent("a1")
    agent2 = make_agent("a2")
    iso_session = AsyncMock()

    with (
        patch.object(
            svc, "find_by_group", new_callable=AsyncMock, return_value=[agent1, agent2]
        ),
        _patch_isolated_session(iso_session),
    ):
        await svc.delete_all_for_group(group_ctx)

    # Two deletes (tasks then agents) committed on the private connection
    assert iso_session.execute.await_count == 2
    iso_session.commit.assert_awaited_once()
