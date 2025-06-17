"""
Unit tests for CrewService.

Tests the functionality of crew management service including
CRUD operations, group isolation, and data serialization.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from uuid import UUID, uuid4
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.crew_service import CrewService
from src.models.crew import Crew
from src.repositories.crew_repository import CrewRepository
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import GroupContext


# Mock crew model
class MockCrew:
    def __init__(self, id=None, name="Test Crew",
                 agent_ids=None, task_ids=None, nodes=None, edges=None,
                 group_id="group-123", created_by_email="test@example.com",
                 created_at=None, updated_at=None):
        self.id = id or uuid4()
        self.name = name
        self.agent_ids = agent_ids or []
        self.task_ids = task_ids or []
        self.nodes = nodes or []
        self.edges = edges or []
        self.group_id = group_id
        self.created_by_email = created_by_email
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def mock_repository():
    """Create a mock crew repository."""
    return AsyncMock(spec=CrewRepository)


@pytest.fixture
def crew_service(mock_session, mock_repository):
    """Create a crew service with mocked dependencies."""
    with patch('src.services.crew_service.CrewRepository', return_value=mock_repository):
        service = CrewService(session=mock_session)
        service.repository = mock_repository
        return service


@pytest.fixture
def sample_crew_create():
    """Create a sample crew creation schema."""
    return CrewCreate(
        name="New Crew",
        agent_ids=["agent-1", "agent-2"],
        task_ids=["task-1", "task-2"],
        nodes=[{
            "id": "node-1", 
            "type": "agent",
            "position": {"x": 100, "y": 100},
            "data": {"label": "Test Agent"}
        }],
        edges=[{
            "id": "edge-1",
            "source": "node-1", 
            "target": "node-2"
        }]
    )


@pytest.fixture
def sample_crew_update():
    """Create a sample crew update schema."""
    return CrewUpdate(
        name="Updated Crew",
        agent_ids=["agent-1", "agent-3"],
        task_ids=["task-1", "task-3"]
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


class TestCrewServiceInit:
    """Test cases for CrewService initialization."""
    
    def test_init_success(self, mock_session):
        """Test successful initialization."""
        service = CrewService(session=mock_session)
        
        assert service.session == mock_session
        assert isinstance(service.repository, CrewRepository)


class TestCrewServiceGet:
    """Test cases for get method."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, crew_service, mock_repository):
        """Test successful crew retrieval."""
        crew_id = uuid4()
        crew = MockCrew(id=crew_id)
        mock_repository.get.return_value = crew
        
        result = await crew_service.get(crew_id)
        
        assert result == crew
        mock_repository.get.assert_called_once_with(crew_id)
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, crew_service, mock_repository):
        """Test get when crew is not found."""
        crew_id = uuid4()
        mock_repository.get.return_value = None
        
        result = await crew_service.get(crew_id)
        
        assert result is None
        mock_repository.get.assert_called_once_with(crew_id)


class TestCrewServiceCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, crew_service, mock_repository, sample_crew_create):
        """Test successful crew creation."""
        created_crew = MockCrew(
            name=sample_crew_create.name
        )
        mock_repository.create.return_value = created_crew
        
        result = await crew_service.create(sample_crew_create)
        
        assert result == created_crew
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["name"] == "New Crew"


class TestCrewServiceFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, crew_service, mock_repository):
        """Test successful find by name."""
        crew = MockCrew(name="Specific Crew")
        mock_repository.find_by_name.return_value = crew
        
        result = await crew_service.find_by_name("Specific Crew")
        
        assert result == crew
        mock_repository.find_by_name.assert_called_once_with("Specific Crew")
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, crew_service, mock_repository):
        """Test find by name when crew doesn't exist."""
        mock_repository.find_by_name.return_value = None
        
        result = await crew_service.find_by_name("Non-existent Crew")
        
        assert result is None
        mock_repository.find_by_name.assert_called_once_with("Non-existent Crew")


class TestCrewServiceFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, crew_service, mock_repository):
        """Test successful find all crews."""
        crews = [
            MockCrew(id=uuid4(), name="Crew 1"),
            MockCrew(id=uuid4(), name="Crew 2"),
            MockCrew(id=uuid4(), name="Crew 3")
        ]
        mock_repository.find_all.return_value = crews
        
        result = await crew_service.find_all()
        
        assert result == crews
        assert len(result) == 3
        mock_repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, crew_service, mock_repository):
        """Test find all when no crews exist."""
        mock_repository.find_all.return_value = []
        
        result = await crew_service.find_all()
        
        assert result == []
        mock_repository.find_all.assert_called_once()


class TestCrewServiceUpdateWithPartialData:
    """Test cases for update_with_partial_data method."""
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_success(self, crew_service, mock_repository, sample_crew_update):
        """Test successful partial update."""
        crew_id = uuid4()
        updated_crew = MockCrew(
            id=crew_id,
            name=sample_crew_update.name
        )
        mock_repository.update.return_value = updated_crew
        
        result = await crew_service.update_with_partial_data(crew_id, sample_crew_update)
        
        assert result == updated_crew
        mock_repository.update.assert_called_once()
        call_args = mock_repository.update.call_args[0]
        assert call_args[0] == crew_id
        assert "name" in call_args[1]
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_no_fields(self, crew_service, mock_repository):
        """Test update with no fields set (all None)."""
        crew_id = uuid4()
        empty_update = CrewUpdate()
        existing_crew = MockCrew(id=crew_id)
        mock_repository.get.return_value = existing_crew
        
        result = await crew_service.update_with_partial_data(crew_id, empty_update)
        
        assert result == existing_crew
        mock_repository.update.assert_not_called()
        mock_repository.get.assert_called_once_with(crew_id)
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_not_found(self, crew_service, mock_repository, sample_crew_update):
        """Test update when crew is not found."""
        crew_id = uuid4()
        mock_repository.update.return_value = None
        
        result = await crew_service.update_with_partial_data(crew_id, sample_crew_update)
        
        assert result is None
        mock_repository.update.assert_called_once()


class TestCrewServiceCreateCrew:
    """Test cases for create_crew method."""
    
    @pytest.mark.asyncio
    async def test_create_crew_success(self, crew_service, mock_repository, sample_crew_create):
        """Test successful crew creation with data processing."""
        created_crew = MockCrew(name=sample_crew_create.name)
        mock_repository.create.return_value = created_crew
        
        with patch('src.services.crew_service.logger') as mock_logger:
            result = await crew_service.create_crew(sample_crew_create)
            
            assert result == created_crew
            mock_repository.create.assert_called_once()
            call_args = mock_repository.create.call_args[0][0]
            assert call_args["name"] == "New Crew"
            assert call_args["agent_ids"] == ["agent-1", "agent-2"]
            assert call_args["task_ids"] == ["task-1", "task-2"]
            assert len(call_args["nodes"]) == 1
            assert len(call_args["edges"]) == 1
            
            # Check logging calls
            assert mock_logger.info.call_count >= 4
    
    @pytest.mark.asyncio
    async def test_create_crew_with_none_lists(self, crew_service, mock_repository):
        """Test crew creation with empty lists (default values)."""
        crew_data = CrewCreate(
            name="Minimal Crew"
            # agent_ids, task_ids, nodes, and edges will default to empty lists
        )
        created_crew = MockCrew(name="Minimal Crew")
        mock_repository.create.return_value = created_crew
        
        result = await crew_service.create_crew(crew_data)
        
        assert result == created_crew
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["agent_ids"] == []
        assert call_args["task_ids"] == []
        assert call_args["nodes"] == []
        assert call_args["edges"] == []
    
    @pytest.mark.asyncio
    async def test_create_crew_with_uuid_ids(self, crew_service, mock_repository):
        """Test crew creation with UUID objects in agent_ids and task_ids."""
        agent_uuid = uuid4()
        task_uuid = uuid4()
        crew_data = CrewCreate(
            name="UUID Crew",
            agent_ids=[str(agent_uuid)],
            task_ids=[str(task_uuid)],
            nodes=[],
            edges=[]
        )
        created_crew = MockCrew(name="UUID Crew")
        mock_repository.create.return_value = created_crew
        
        result = await crew_service.create_crew(crew_data)
        
        assert result == created_crew
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["agent_ids"] == [str(agent_uuid)]
        assert call_args["task_ids"] == [str(task_uuid)]
    
    @pytest.mark.asyncio
    async def test_create_crew_exception_handling(self, crew_service, mock_repository, sample_crew_create):
        """Test crew creation exception handling."""
        mock_repository.create.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            with patch('src.services.crew_service.logger') as mock_logger:
                await crew_service.create_crew(sample_crew_create)
                
                mock_logger.error.assert_called_once()


class TestCrewServiceDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, crew_service, mock_repository):
        """Test successful crew deletion."""
        crew_id = uuid4()
        mock_repository.delete.return_value = True
        
        result = await crew_service.delete(crew_id)
        
        assert result is True
        mock_repository.delete.assert_called_once_with(crew_id)
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, crew_service, mock_repository):
        """Test delete when crew is not found."""
        crew_id = uuid4()
        mock_repository.delete.return_value = False
        
        result = await crew_service.delete(crew_id)
        
        assert result is False
        mock_repository.delete.assert_called_once_with(crew_id)


class TestCrewServiceDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, crew_service, mock_repository):
        """Test successful delete all crews."""
        mock_repository.delete_all.return_value = None
        
        await crew_service.delete_all()
        
        mock_repository.delete_all.assert_called_once()


class TestCrewServiceCreateWithGroup:
    """Test cases for create_with_group method."""
    
    @pytest.mark.asyncio
    async def test_create_with_group_success(self, crew_service, mock_repository, 
                                            sample_crew_create, sample_group_context):
        """Test successful crew creation with group context."""
        created_crew = MockCrew(
            name=sample_crew_create.name,
            group_id=sample_group_context.primary_group_id,
            created_by_email=sample_group_context.group_email
        )
        mock_repository.create.return_value = created_crew
        
        with patch('src.services.crew_service.logger') as mock_logger:
            result = await crew_service.create_with_group(sample_crew_create, sample_group_context)
            
            assert result == created_crew
            mock_repository.create.assert_called_once()
            call_args = mock_repository.create.call_args[0][0]
            assert call_args["group_id"] == "group-123"  # Should use primary_group_id property
            assert call_args["created_by_email"] == "test@example.com"
            assert call_args["name"] == "New Crew"
            
            # Check logging
            assert mock_logger.info.call_count >= 4
    
    @pytest.mark.asyncio
    async def test_create_with_group_exception_handling(self, crew_service, mock_repository, 
                                                       sample_crew_create, sample_group_context):
        """Test create with group exception handling."""
        mock_repository.create.side_effect = Exception("Group creation error")
        
        with pytest.raises(Exception, match="Group creation error"):
            with patch('src.services.crew_service.logger') as mock_logger:
                await crew_service.create_with_group(sample_crew_create, sample_group_context)
                
                mock_logger.error.assert_called_once()


class TestCrewServiceFindByGroup:
    """Test cases for find_by_group method."""
    
    @pytest.mark.asyncio
    async def test_find_by_group_success(self, crew_service, mock_repository, sample_group_context):
        """Test successful find crews by group."""
        crews = [
            MockCrew(id=uuid4(), group_id="group-123"),
            MockCrew(id=uuid4(), group_id="group-123"),
            MockCrew(id=uuid4(), group_id="group-456")
        ]
        mock_repository.find_by_group.return_value = crews
        
        result = await crew_service.find_by_group(sample_group_context)
        
        assert result == crews
        mock_repository.find_by_group.assert_called_once_with(["group-123", "group-456"])
    
    @pytest.mark.asyncio
    async def test_find_by_group_empty_group_context(self, crew_service):
        """Test find by group with empty group IDs."""
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        result = await crew_service.find_by_group(empty_context)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_find_by_group_no_crews(self, crew_service, mock_repository, sample_group_context):
        """Test find by group when no crews exist for the group."""
        mock_repository.find_by_group.return_value = []
        
        result = await crew_service.find_by_group(sample_group_context)
        
        assert result == []
        mock_repository.find_by_group.assert_called_once_with(["group-123", "group-456"])


class TestCrewServiceGetByGroup:
    """Test cases for get_by_group method."""
    
    @pytest.mark.asyncio
    async def test_get_by_group_success(self, crew_service, mock_repository, sample_group_context):
        """Test successful get crew by group."""
        crew_id = uuid4()
        crew = MockCrew(id=crew_id, group_id="group-123")
        mock_repository.get_by_group.return_value = crew
        
        result = await crew_service.get_by_group(crew_id, sample_group_context)
        
        assert result == crew
        mock_repository.get_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])
    
    @pytest.mark.asyncio
    async def test_get_by_group_empty_group_context(self, crew_service):
        """Test get by group with empty group IDs."""
        crew_id = uuid4()
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        result = await crew_service.get_by_group(crew_id, empty_context)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_by_group_not_found(self, crew_service, mock_repository, sample_group_context):
        """Test get by group when crew not found or doesn't belong to group."""
        crew_id = uuid4()
        mock_repository.get_by_group.return_value = None
        
        result = await crew_service.get_by_group(crew_id, sample_group_context)
        
        assert result is None
        mock_repository.get_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])


class TestCrewServiceUpdateWithPartialDataByGroup:
    """Test cases for update_with_partial_data_by_group method."""
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_by_group_success(self, crew_service, mock_repository, 
                                                            sample_crew_update, sample_group_context):
        """Test successful partial update by group."""
        crew_id = uuid4()
        existing_crew = MockCrew(id=crew_id, group_id="group-123")
        updated_crew = MockCrew(
            id=crew_id,
            name=sample_crew_update.name,
            group_id="group-123"
        )
        
        mock_repository.get_by_group.return_value = existing_crew
        mock_repository.update.return_value = updated_crew
        
        result = await crew_service.update_with_partial_data_by_group(crew_id, sample_crew_update, sample_group_context)
        
        assert result == updated_crew
        mock_repository.get_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])
        mock_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_by_group_empty_context(self, crew_service, sample_crew_update):
        """Test update by group with empty group context."""
        crew_id = uuid4()
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        result = await crew_service.update_with_partial_data_by_group(crew_id, sample_crew_update, empty_context)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_by_group_not_found(self, crew_service, mock_repository, 
                                                              sample_crew_update, sample_group_context):
        """Test update by group when crew not found."""
        crew_id = uuid4()
        mock_repository.get_by_group.return_value = None
        
        result = await crew_service.update_with_partial_data_by_group(crew_id, sample_crew_update, sample_group_context)
        
        assert result is None
        mock_repository.get_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])
        mock_repository.update.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_with_partial_data_by_group_no_fields(self, crew_service, mock_repository, sample_group_context):
        """Test update by group with no fields to update."""
        crew_id = uuid4()
        existing_crew = MockCrew(id=crew_id, group_id="group-123")
        empty_update = CrewUpdate()
        
        mock_repository.get_by_group.return_value = existing_crew
        
        result = await crew_service.update_with_partial_data_by_group(crew_id, empty_update, sample_group_context)
        
        assert result == existing_crew
        mock_repository.get_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])
        mock_repository.update.assert_not_called()


class TestCrewServiceDeleteByGroup:
    """Test cases for delete_by_group method."""
    
    @pytest.mark.asyncio
    async def test_delete_by_group_success(self, crew_service, mock_repository, sample_group_context):
        """Test successful delete by group."""
        crew_id = uuid4()
        mock_repository.delete_by_group.return_value = True
        
        result = await crew_service.delete_by_group(crew_id, sample_group_context)
        
        assert result is True
        mock_repository.delete_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])
    
    @pytest.mark.asyncio
    async def test_delete_by_group_empty_context(self, crew_service):
        """Test delete by group with empty group context."""
        crew_id = uuid4()
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        result = await crew_service.delete_by_group(crew_id, empty_context)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_by_group_not_found(self, crew_service, mock_repository, sample_group_context):
        """Test delete by group when crew not found or doesn't belong to group."""
        crew_id = uuid4()
        mock_repository.delete_by_group.return_value = False
        
        result = await crew_service.delete_by_group(crew_id, sample_group_context)
        
        assert result is False
        mock_repository.delete_by_group.assert_called_once_with(crew_id, ["group-123", "group-456"])


class TestCrewServiceDeleteAllByGroup:
    """Test cases for delete_all_by_group method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_by_group_success(self, crew_service, mock_repository, sample_group_context):
        """Test successful delete all by group."""
        mock_repository.delete_all_by_group.return_value = None
        
        await crew_service.delete_all_by_group(sample_group_context)
        
        mock_repository.delete_all_by_group.assert_called_once_with(["group-123", "group-456"])
    
    @pytest.mark.asyncio
    async def test_delete_all_by_group_empty_context(self, crew_service, mock_repository):
        """Test delete all by group with empty group context."""
        empty_context = GroupContext(
            group_ids=[],
            group_email="test@example.com",
            email_domain="example.com",
            user_id="user-123"
        )
        
        await crew_service.delete_all_by_group(empty_context)
        
        mock_repository.delete_all_by_group.assert_not_called()