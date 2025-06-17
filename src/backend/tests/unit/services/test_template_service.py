"""
Unit tests for TemplateService.

Tests the functionality of template management service including
CRUD operations, UnitOfWork pattern usage, and default template handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List

from src.services.template_service import TemplateService
from src.models.template import PromptTemplate
from src.repositories.template_repository import TemplateRepository
from src.schemas.template import PromptTemplateCreate, PromptTemplateUpdate


# Mock template model
class MockPromptTemplate:
    def __init__(self, id=1, name="test_template", template="Test template content",
                 description="Test template description", is_active=True,
                 created_at=None, updated_at=None):
        self.id = id
        self.name = name
        self.template = template
        self.description = description
        self.is_active = is_active
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


@pytest.fixture
def mock_repository():
    """Create a mock template repository."""
    return AsyncMock(spec=TemplateRepository)


@pytest.fixture
def template_service(mock_repository):
    """Create a template service with mocked repository."""
    return TemplateService(repository=mock_repository)


@pytest.fixture
def sample_template_create():
    """Create a sample template creation schema."""
    return PromptTemplateCreate(
        name="new_template",
        template="New template content: {variable}",
        description="A new test template"
    )


@pytest.fixture
def sample_template_update():
    """Create a sample template update schema."""
    return PromptTemplateUpdate(
        name="updated_template",
        template="Updated template content: {variable}",
        description="Updated test template"
    )


@pytest.fixture
def sample_templates():
    """Create sample templates for testing."""
    return [
        MockPromptTemplate(id=1, name="template1", template="Content 1"),
        MockPromptTemplate(id=2, name="template2", template="Content 2"),
        MockPromptTemplate(id=3, name="template3", template="Content 3")
    ]


class TestTemplateServiceInit:
    """Test cases for TemplateService initialization."""
    
    def test_init_success(self, mock_repository):
        """Test successful initialization."""
        service = TemplateService(repository=mock_repository)
        
        assert service.repository == mock_repository
    
    def test_create_factory_method(self):
        """Test the create factory method."""
        # Since SessionLocal is imported inside the method, we need to patch it differently
        with patch('src.db.session.SessionLocal') as mock_session_local:
            with patch.object(TemplateService, '__init__', return_value=None) as mock_init:
                mock_session = MagicMock()
                mock_session_local.return_value = mock_session
                
                service = TemplateService.create()
                
                # Verify SessionLocal was called
                mock_session_local.assert_called_once()
                # Verify the constructor was called with a repository
                mock_init.assert_called_once()


class TestTemplateServiceFindAll:
    """Test cases for find_all methods."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, template_service, mock_repository, sample_templates):
        """Test successful find all templates."""
        mock_repository.find_active_templates.return_value = sample_templates
        
        result = await template_service.find_all()
        
        assert result == sample_templates
        assert len(result) == 3
        mock_repository.find_active_templates.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, template_service, mock_repository):
        """Test find all when no templates exist."""
        mock_repository.find_active_templates.return_value = []
        
        result = await template_service.find_all()
        
        assert result == []
        mock_repository.find_active_templates.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_templates_class_method(self, sample_templates):
        """Test find_all_templates class method using UnitOfWork."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_all', return_value=sample_templates):
                    result = await TemplateService.find_all_templates()
                    
                    assert result == sample_templates


class TestTemplateServiceGet:
    """Test cases for get methods."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, template_service, mock_repository):
        """Test successful template retrieval."""
        template = MockPromptTemplate(id=1, name="get_test")
        mock_repository.get.return_value = template
        
        result = await template_service.get(1)
        
        assert result == template
        mock_repository.get.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, template_service, mock_repository):
        """Test get when template is not found."""
        mock_repository.get.return_value = None
        
        result = await template_service.get(999)
        
        assert result is None
        mock_repository.get.assert_called_once_with(999)
    
    @pytest.mark.asyncio
    async def test_get_template_by_id_class_method(self):
        """Test get_template_by_id class method using UnitOfWork."""
        template = MockPromptTemplate(id=1)
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'get', return_value=template):
                    result = await TemplateService.get_template_by_id(1)
                    
                    assert result == template


class TestTemplateServiceFindByName:
    """Test cases for find_by_name methods."""
    
    @pytest.mark.asyncio
    async def test_find_by_name_success(self, template_service, mock_repository):
        """Test successful find by name."""
        template = MockPromptTemplate(name="specific_template")
        mock_repository.find_by_name.return_value = template
        
        result = await template_service.find_by_name("specific_template")
        
        assert result == template
        mock_repository.find_by_name.assert_called_once_with("specific_template")
    
    @pytest.mark.asyncio
    async def test_find_by_name_not_found(self, template_service, mock_repository):
        """Test find by name when template doesn't exist."""
        mock_repository.find_by_name.return_value = None
        
        result = await template_service.find_by_name("nonexistent_template")
        
        assert result is None
        mock_repository.find_by_name.assert_called_once_with("nonexistent_template")
    
    @pytest.mark.asyncio
    async def test_find_template_by_name_class_method(self):
        """Test find_template_by_name class method using UnitOfWork."""
        template = MockPromptTemplate(name="class_method_template")
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_by_name', return_value=template):
                    result = await TemplateService.find_template_by_name("class_method_template")
                    
                    assert result == template


class TestTemplateServiceCreate:
    """Test cases for create methods."""
    
    @pytest.mark.asyncio
    async def test_create_template_success(self, template_service, mock_repository, sample_template_create):
        """Test successful template creation."""
        created_template = MockPromptTemplate(
            name=sample_template_create.name,
            template=sample_template_create.template
        )
        mock_repository.create.return_value = created_template
        
        result = await template_service.create_template(sample_template_create)
        
        assert result == created_template
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["name"] == "new_template"
        assert call_args["template"] == "New template content: {variable}"
        assert call_args["description"] == "A new test template"
    
    @pytest.mark.asyncio
    async def test_create_new_template_class_method(self, sample_template_create):
        """Test create_new_template class method using UnitOfWork."""
        created_template = MockPromptTemplate(name=sample_template_create.name)
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'create_template', return_value=created_template):
                    result = await TemplateService.create_new_template(sample_template_create)
                    
                    assert result == created_template
                    mock_uow.commit.assert_called_once()


class TestTemplateServiceUpdate:
    """Test cases for update methods."""
    
    @pytest.mark.asyncio
    async def test_update_template_success(self, template_service, mock_repository, sample_template_update):
        """Test successful template update."""
        updated_template = MockPromptTemplate(
            id=1,
            name=sample_template_update.name,
            template=sample_template_update.template
        )
        mock_repository.update_template.return_value = updated_template
        
        result = await template_service.update_template(1, sample_template_update)
        
        assert result == updated_template
        mock_repository.update_template.assert_called_once()
        call_args = mock_repository.update_template.call_args[0]
        assert call_args[0] == 1
        update_data = call_args[1]
        assert update_data["name"] == "updated_template"
        assert update_data["template"] == "Updated template content: {variable}"
    
    @pytest.mark.asyncio
    async def test_update_template_not_found(self, template_service, mock_repository, sample_template_update):
        """Test update when template is not found."""
        mock_repository.update_template.return_value = None
        
        result = await template_service.update_template(999, sample_template_update)
        
        assert result is None
        mock_repository.update_template.assert_called_once_with(999, sample_template_update.model_dump(exclude_unset=True))
    
    @pytest.mark.asyncio
    async def test_update_template_partial_data(self, template_service, mock_repository):
        """Test update with partial data."""
        partial_update = PromptTemplateUpdate(name="partial_update_only")
        updated_template = MockPromptTemplate(id=1, name="partial_update_only")
        mock_repository.update_template.return_value = updated_template
        
        result = await template_service.update_template(1, partial_update)
        
        assert result == updated_template
        call_args = mock_repository.update_template.call_args[0][1]
        assert "name" in call_args
        assert "template" not in call_args  # Should exclude unset fields
        assert "description" not in call_args
    
    @pytest.mark.asyncio
    async def test_update_existing_template_class_method(self, sample_template_update):
        """Test update_existing_template class method using UnitOfWork."""
        updated_template = MockPromptTemplate(id=1, name=sample_template_update.name)
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'update_template', return_value=updated_template):
                    result = await TemplateService.update_existing_template(1, sample_template_update)
                    
                    assert result == updated_template
                    mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_existing_template_not_found_no_commit(self, sample_template_update):
        """Test update_existing_template class method when template not found."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'update_template', return_value=None):
                    result = await TemplateService.update_existing_template(999, sample_template_update)
                    
                    assert result is None
                    mock_uow.commit.assert_not_called()  # Should not commit if template not found


class TestTemplateServiceDelete:
    """Test cases for delete methods."""
    
    @pytest.mark.asyncio
    async def test_delete_template_success(self, template_service, mock_repository):
        """Test successful template deletion."""
        mock_repository.delete.return_value = True
        
        result = await template_service.delete_template(1)
        
        assert result is True
        mock_repository.delete.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_delete_template_not_found(self, template_service, mock_repository):
        """Test delete when template is not found."""
        mock_repository.delete.return_value = False
        
        result = await template_service.delete_template(999)
        
        assert result is False
        mock_repository.delete.assert_called_once_with(999)
    
    @pytest.mark.asyncio
    async def test_delete_template_by_id_class_method(self):
        """Test delete_template_by_id class method using UnitOfWork."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'delete_template', return_value=True):
                    result = await TemplateService.delete_template_by_id(1)
                    
                    assert result is True
                    mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_template_by_id_not_found_no_commit(self):
        """Test delete_template_by_id class method when template not found."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'delete_template', return_value=False):
                    result = await TemplateService.delete_template_by_id(999)
                    
                    assert result is False
                    mock_uow.commit.assert_not_called()  # Should not commit if template not found


class TestTemplateServiceDeleteAll:
    """Test cases for delete_all methods."""
    
    @pytest.mark.asyncio
    async def test_delete_all_templates_success(self, template_service, mock_repository):
        """Test successful delete all templates."""
        mock_repository.delete_all.return_value = 3
        
        result = await template_service.delete_all_templates()
        
        assert result == 3
        mock_repository.delete_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_templates_empty(self, template_service, mock_repository):
        """Test delete all when no templates exist."""
        mock_repository.delete_all.return_value = 0
        
        result = await template_service.delete_all_templates()
        
        assert result == 0
        mock_repository.delete_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_templates_service_class_method(self):
        """Test delete_all_templates_service class method using UnitOfWork."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'delete_all_templates', return_value=5):
                    result = await TemplateService.delete_all_templates_service()
                    
                    assert result == 5
                    mock_uow.commit.assert_called_once()


class TestTemplateServiceReset:
    """Test cases for reset methods."""
    
    @pytest.mark.asyncio
    async def test_reset_templates_success(self, template_service, mock_repository):
        """Test successful templates reset."""
        mock_repository.delete_all.return_value = 3
        
        with patch('src.services.template_service.DEFAULT_TEMPLATES', [
            {"name": "default1", "template": "content1", "description": "desc1"},
            {"name": "default2", "template": "content2", "description": "desc2"}
        ]):
            with patch.object(template_service, 'create_template') as mock_create:
                mock_create.return_value = MockPromptTemplate()
                
                result = await template_service.reset_templates()
                
                assert result == 2  # Two default templates created
                mock_repository.delete_all.assert_called_once()
                assert mock_create.call_count == 2
    
    @pytest.mark.asyncio
    async def test_reset_templates_service_class_method(self):
        """Test reset_templates_service class method using UnitOfWork."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        mock_uow.commit = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'reset_templates', return_value=3):
                    result = await TemplateService.reset_templates_service()
                    
                    assert result == 3
                    mock_uow.commit.assert_called_once()


class TestTemplateServiceGetContent:
    """Test cases for get_template_content method."""
    
    @pytest.mark.asyncio
    async def test_get_template_content_success(self):
        """Test successful template content retrieval."""
        template = MockPromptTemplate(name="content_template", template="Template content here")
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_by_name', return_value=template):
                    result = await TemplateService.get_template_content("content_template")
                    
                    assert result == "Template content here"
    
    @pytest.mark.asyncio
    async def test_get_template_content_not_found_with_default(self):
        """Test template content retrieval when not found but default provided."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_by_name', return_value=None):
                    result = await TemplateService.get_template_content(
                        "nonexistent", 
                        default_template="Default content"
                    )
                    
                    assert result == "Default content"
    
    @pytest.mark.asyncio
    async def test_get_template_content_not_found_no_default(self):
        """Test template content retrieval when not found and no default."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_by_name', return_value=None):
                    with patch('src.services.template_service.logger') as mock_logger:
                        result = await TemplateService.get_template_content("nonexistent")
                        
                        assert result == ""
                        mock_logger.warning.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_template_content_exception_with_default(self):
        """Test template content retrieval when exception occurs but default provided."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_by_name', side_effect=Exception("Database error")):
                    with patch('src.services.template_service.logger') as mock_logger:
                        result = await TemplateService.get_template_content(
                            "error_template", 
                            default_template="Fallback content"
                        )
                        
                        assert result == "Fallback content"
                        mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_template_content_exception_no_default(self):
        """Test template content retrieval when exception occurs and no default."""
        mock_uow = AsyncMock()
        mock_uow.template_repository = AsyncMock()
        
        with patch('src.services.template_service.UnitOfWork') as mock_uow_class:
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            
            with patch.object(TemplateService, '__init__', return_value=None):
                with patch.object(TemplateService, 'find_by_name', side_effect=Exception("Database error")):
                    with patch('src.services.template_service.logger') as mock_logger:
                        result = await TemplateService.get_template_content("error_template")
                        
                        assert result == ""
                        mock_logger.error.assert_called_once()