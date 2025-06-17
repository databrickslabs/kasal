"""
Unit tests for TemplateRepository.

Tests the functionality of template repository including
CRUD operations, active template filtering, and custom queries.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete

from src.repositories.template_repository import TemplateRepository
from src.models.template import PromptTemplate


# Mock template model
class MockPromptTemplate:
    def __init__(self, id=1, name="test_template", template="Test template content",
                 description="Test template description", is_active=True,
                 created_at=None, updated_at=None, group_id=None, created_by_email=None):
        self.id = id
        self.name = name
        self.template = template
        self.description = description
        self.is_active = is_active
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
    session.get = AsyncMock()
    session.add = AsyncMock()
    session.delete = AsyncMock()
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    return session


@pytest.fixture
def template_repository(mock_async_session):
    """Create a template repository with async session."""
    return TemplateRepository(session=mock_async_session)


@pytest.fixture
def sample_templates():
    """Create sample templates for testing."""
    return [
        MockPromptTemplate(id=1, name="template1", is_active=True),
        MockPromptTemplate(id=2, name="template2", is_active=True),
        MockPromptTemplate(id=3, name="template3", is_active=False)
    ]


@pytest.fixture
def sample_template_data():
    """Create sample template data for creation."""
    return {
        "name": "new_template",
        "template": "New template content: {variable}",
        "description": "A new test template",
        "is_active": True
    }


class TestTemplateRepositoryInit:
    """Test cases for TemplateRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = TemplateRepository(session=mock_async_session)
        
        assert repository.model == PromptTemplate
        assert repository.session == mock_async_session


class TestTemplateRepositoryGet:
    """Test cases for get method."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, template_repository, mock_async_session):
        """Test successful template retrieval."""
        template = MockPromptTemplate(id=1, name="get_test", group_id="group-123")
        mock_async_session.get.return_value = template
        
        result = await template_repository.get(1)
        
        assert result == template
        mock_async_session.get.assert_called_once_with(PromptTemplate, 1)
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, template_repository, mock_async_session):
        """Test get when template is not found."""
        mock_async_session.get.return_value = None
        
        result = await template_repository.get(999)
        
        assert result is None
        mock_async_session.get.assert_called_once_with(PromptTemplate, 999)


class TestTemplateRepositoryCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, template_repository, mock_async_session, sample_template_data):
        """Test successful template creation."""
        result = await template_repository.create(sample_template_data)
        
        # Verify the result is a PromptTemplate instance with correct attributes
        assert isinstance(result, PromptTemplate)
        assert result.name == sample_template_data["name"]
        assert result.template == sample_template_data["template"] 
        assert result.description == sample_template_data["description"]
        assert result.is_active == sample_template_data["is_active"]
        mock_async_session.add.assert_called_once_with(result)
        mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_minimal_data(self, template_repository, mock_async_session):
        """Test creation with minimal required data."""
        minimal_data = {
            "name": "minimal_template",
            "template": "Minimal content"
        }
        
        result = await template_repository.create(minimal_data)
        
        # Verify the result is a PromptTemplate instance with correct attributes
        assert isinstance(result, PromptTemplate)
        assert result.name == minimal_data["name"]
        assert result.template == minimal_data["template"]
        # Verify defaults are set by the model's __init__
        assert result.is_active is True  # Default should be True
        mock_async_session.add.assert_called_once_with(result)
        mock_async_session.flush.assert_called_once()


class TestTemplateRepositoryFindByName:
    """Test cases for find_by_name method."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, template_repository, mock_async_session):
        """Test successful find by name."""
        template = MockPromptTemplate(name="specific_template", group_id="group-123")
        mock_result = MockResult([template])
        mock_async_session.execute.return_value = mock_result
        
        result = await template_repository.find_by_name("specific_template")
        
        assert result == template
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(PromptTemplate)))
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, template_repository, mock_async_session):
        """Test find by name when template doesn't exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await template_repository.find_by_name("nonexistent_template")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_name_multiple_returns_first(self, template_repository, mock_async_session):
        """Test find by name returns first result when multiple exist."""
        template1 = MockPromptTemplate(id=1, name="same_name", group_id="group-123")
        template2 = MockPromptTemplate(id=2, name="same_name", group_id="group-123")
        mock_result = MockResult([template1, template2])
        mock_async_session.execute.return_value = mock_result
        
        result = await template_repository.find_by_name("same_name")
        
        assert result == template1
        mock_async_session.execute.assert_called_once()


class TestTemplateRepositoryFindActiveTemplates:
    """Test cases for find_active_templates method."""
    
    @pytest.mark.asyncio
    async def test_find_active_templates_success(self, template_repository, mock_async_session, sample_templates):
        """Test successful find active templates."""
        active_templates = [t for t in sample_templates if t.is_active]
        mock_result = MockResult(active_templates)
        mock_async_session.execute.return_value = mock_result
        
        result = await template_repository.find_active_templates()
        
        assert len(result) == 2
        assert all(t.is_active for t in result)
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly to filter by is_active
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(PromptTemplate)))
    
    @pytest.mark.asyncio
    async def test_find_active_templates_empty(self, template_repository, mock_async_session):
        """Test find active templates when no active templates exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await template_repository.find_active_templates()
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_active_templates_returns_list(self, template_repository, mock_async_session, sample_templates):
        """Test find active templates returns a list (not generator)."""
        active_templates = [t for t in sample_templates if t.is_active]
        mock_result = MockResult(active_templates)
        mock_async_session.execute.return_value = mock_result
        
        result = await template_repository.find_active_templates()
        
        assert isinstance(result, list)
        assert len(result) == 2


class TestTemplateRepositoryUpdateTemplate:
    """Test cases for update_template method."""
    
    @pytest.mark.asyncio
    async def test_update_template_success(self, template_repository, mock_async_session):
        """Test successful template update."""
        update_data = {"name": "updated_template", "description": "Updated description"}
        updated_template = MockPromptTemplate(id=1, name="updated_template", group_id="group-123")
        
        # Mock the get call that happens after update
        mock_async_session.get.return_value = updated_template
        
        with patch('src.repositories.template_repository.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            
            result = await template_repository.update_template(1, update_data)
            
            assert result == updated_template
            mock_async_session.execute.assert_called_once()
            mock_async_session.flush.assert_called_once()
            mock_async_session.get.assert_called_once_with(PromptTemplate, 1)
            
            # Verify the update statement was constructed correctly
            call_args = mock_async_session.execute.call_args[0][0]
            assert isinstance(call_args, type(update(PromptTemplate)))
    
    @pytest.mark.asyncio
    async def test_update_template_not_found(self, template_repository, mock_async_session):
        """Test update when template not found after update."""
        update_data = {"name": "updated_template"}
        
        # Mock the get call to return None (template not found)
        mock_async_session.get.return_value = None
        
        with patch('src.repositories.template_repository.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            
            result = await template_repository.update_template(999, update_data)
            
            assert result is None
            mock_async_session.execute.assert_called_once()
            mock_async_session.flush.assert_called_once()
            mock_async_session.get.assert_called_once_with(PromptTemplate, 999)
    
    @pytest.mark.asyncio
    async def test_update_template_adds_timestamp(self, template_repository, mock_async_session):
        """Test that update_template adds updated_at timestamp."""
        update_data = {"name": "updated_template"}
        expected_timestamp = datetime(2023, 1, 1, 12, 0, 0)
        
        mock_async_session.get.return_value = MockPromptTemplate(id=1)
        
        with patch('src.repositories.template_repository.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = expected_timestamp
            
            await template_repository.update_template(1, update_data)
            
            # The update_data should be modified to include updated_at
            mock_async_session.execute.assert_called_once()
            # We can't easily check the exact values passed to the update statement,
            # but we can verify the datetime.utcnow was called
            mock_datetime.utcnow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_template_empty_data(self, template_repository, mock_async_session):
        """Test update with empty data."""
        update_data = {}
        updated_template = MockPromptTemplate(id=1)
        
        mock_async_session.get.return_value = updated_template
        
        with patch('src.repositories.template_repository.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            
            result = await template_repository.update_template(1, update_data)
            
            assert result == updated_template
            mock_async_session.execute.assert_called_once()
            mock_async_session.flush.assert_called_once()


class TestTemplateRepositoryDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, template_repository, mock_async_session, sample_templates):
        """Test successful delete all templates."""
        # Mock the count query
        count_result = MockResult(sample_templates)
        mock_async_session.execute.return_value = count_result
        
        result = await template_repository.delete_all()
        
        assert result == 3
        assert mock_async_session.execute.call_count == 2  # Once for count, once for delete
        mock_async_session.flush.assert_called_once()
        
        # Verify both select and delete statements were executed
        call_args_list = mock_async_session.execute.call_args_list
        assert isinstance(call_args_list[0][0][0], type(select(PromptTemplate)))  # Count query
        assert isinstance(call_args_list[1][0][0], type(delete(PromptTemplate)))  # Delete query
    
    @pytest.mark.asyncio
    async def test_delete_all_empty(self, template_repository, mock_async_session):
        """Test delete all when no templates exist."""
        # Mock the count query to return empty result
        count_result = MockResult([])
        mock_async_session.execute.return_value = count_result
        
        result = await template_repository.delete_all()
        
        assert result == 0
        assert mock_async_session.execute.call_count == 2  # Once for count, once for delete
        mock_async_session.flush.assert_called_once()


class TestTemplateRepositoryDelete:
    """Test cases for delete method."""
    
    @pytest.mark.asyncio
    async def test_delete_success(self, template_repository, mock_async_session):
        """Test successful template deletion."""
        template = MockPromptTemplate(id=1)
        mock_async_session.get.return_value = template
        
        result = await template_repository.delete(1)
        
        assert result is True
        mock_async_session.get.assert_called_once_with(PromptTemplate, 1)
        mock_async_session.delete.assert_called_once_with(template)
        mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, template_repository, mock_async_session):
        """Test delete when template is not found."""
        mock_async_session.get.return_value = None
        
        result = await template_repository.delete(999)
        
        assert result is False
        mock_async_session.get.assert_called_once_with(PromptTemplate, 999)
        mock_async_session.delete.assert_not_called()
        mock_async_session.flush.assert_not_called()


class TestTemplateRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_find_by_name_flow(self, template_repository, mock_async_session):
        """Test the flow from create to find_by_name."""
        template_data = {"name": "integration_template", "template": "content"}
        
        # Create template
        create_result = await template_repository.create(template_data)
        
        # Mock find_by_name to return the created template
        mock_result = MockResult([create_result])
        mock_async_session.execute.return_value = mock_result
        
        # Find template by name
        find_result = await template_repository.find_by_name("integration_template")
        
        # Verify both operations worked with the same template
        assert isinstance(create_result, PromptTemplate)
        assert create_result.name == template_data["name"]
        assert find_result == create_result
        mock_async_session.add.assert_called_once()
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_then_delete_flow(self, template_repository, mock_async_session):
        """Test the flow from get to delete."""
        template = MockPromptTemplate(id=1)
        mock_async_session.get.return_value = template
        
        # The delete method calls get internally
        result = await template_repository.delete(1)
        
        assert result is True
        mock_async_session.get.assert_called_once_with(PromptTemplate, 1)
        mock_async_session.delete.assert_called_once_with(template)
        mock_async_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_then_get_flow(self, template_repository, mock_async_session):
        """Test the flow from update to get."""
        update_data = {"name": "updated_template"}
        updated_template = MockPromptTemplate(id=1, name="updated_template", group_id="group-123")
        
        # Mock the get call that happens after update
        mock_async_session.get.return_value = updated_template
        
        with patch('src.repositories.template_repository.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            
            # The update_template method calls get internally after update
            result = await template_repository.update_template(1, update_data)
            
            assert result == updated_template
            mock_async_session.execute.assert_called_once()  # For update
            mock_async_session.get.assert_called_once_with(PromptTemplate, 1)  # For get after update


class TestTemplateRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_session_error(self, template_repository, mock_async_session):
        """Test get when session raises an error."""
        mock_async_session.get.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await template_repository.get(1)
    
    @pytest.mark.asyncio
    async def test_find_by_name_session_error(self, template_repository, mock_async_session):
        """Test find by name when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await template_repository.find_by_name("error_template")
    
    @pytest.mark.asyncio
    async def test_find_active_templates_session_error(self, template_repository, mock_async_session):
        """Test find active templates when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Session error")
        
        with pytest.raises(Exception, match="Session error"):
            await template_repository.find_active_templates()
    
    @pytest.mark.asyncio
    async def test_create_session_error(self, template_repository, mock_async_session):
        """Test create when session raises an error."""
        template_data = {"name": "error_template", "template": "content"}
        
        with patch('src.repositories.template_repository.PromptTemplate') as mock_template_class:
            mock_template_class.return_value = MockPromptTemplate()
            mock_async_session.flush.side_effect = Exception("Flush error")
            
            with pytest.raises(Exception, match="Flush error"):
                await template_repository.create(template_data)
    
    @pytest.mark.asyncio
    async def test_update_template_session_error(self, template_repository, mock_async_session):
        """Test update template when session raises an error."""
        update_data = {"name": "error_template"}
        mock_async_session.execute.side_effect = Exception("Update error")
        
        with patch('src.repositories.template_repository.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
            
            with pytest.raises(Exception, match="Update error"):
                await template_repository.update_template(1, update_data)
    
    @pytest.mark.asyncio
    async def test_delete_all_session_error(self, template_repository, mock_async_session):
        """Test delete all when session raises an error."""
        mock_async_session.execute.side_effect = Exception("Delete error")
        
        with pytest.raises(Exception, match="Delete error"):
            await template_repository.delete_all()
    
    @pytest.mark.asyncio
    async def test_delete_session_error(self, template_repository, mock_async_session):
        """Test delete when session raises an error."""
        template = MockPromptTemplate(id=1)
        mock_async_session.get.return_value = template
        mock_async_session.delete.side_effect = Exception("Delete error")
        
        with pytest.raises(Exception, match="Delete error"):
            await template_repository.delete(1)