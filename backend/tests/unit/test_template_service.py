"""
Unit tests for TemplateService.

Tests the functionality of the template service including
creating, updating, deleting, and managing templates.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.template_service import TemplateService
from src.schemas.template import TemplateCreate, TemplateUpdate
from src.models.template import Template
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
def mock_template_repository():
    """Create a mock template repository."""
    repo = AsyncMock()
    
    # Create mock template objects
    mock_template = MagicMock(spec=Template)
    mock_template.id = uuid.uuid4()
    mock_template.name = "Research Template"
    mock_template.description = "Template for research tasks"
    mock_template.template_type = "task"
    mock_template.content = {
        "agents": [
            {
                "name": "researcher",
                "role": "Research Analyst",
                "goal": "Conduct thorough research"
            }
        ],
        "tasks": [
            {
                "name": "research_task",
                "description": "Research the given topic",
                "agent": "researcher"
            }
        ]
    }
    mock_template.tags = ["research", "analysis"]
    mock_template.is_public = True
    mock_template.created_at = datetime.now(UTC)
    mock_template.updated_at = datetime.now(UTC)
    mock_template.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_template
    repo.list.return_value = [mock_template]
    repo.create.return_value = mock_template
    repo.update.return_value = mock_template
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_template
    repo.search.return_value = [mock_template]
    repo.get_by_type.return_value = [mock_template]
    repo.get_by_tags.return_value = [mock_template]
    
    return repo


@pytest.fixture
def template_create_data():
    """Create test data for template creation."""
    return TemplateCreate(
        name="Test Template",
        description="A test template for unit testing",
        template_type="crew",
        content={
            "agents": [
                {
                    "name": "test_agent",
                    "role": "Test Agent",
                    "goal": "Perform test operations",
                    "backstory": "I am a test agent"
                }
            ],
            "tasks": [
                {
                    "name": "test_task",
                    "description": "Perform a test task",
                    "agent": "test_agent",
                    "expected_output": "Test results"
                }
            ]
        },
        tags=["test", "automation"],
        is_public=False
    )


@pytest.fixture
def template_update_data():
    """Create test data for template updates."""
    return TemplateUpdate(
        name="Updated Template",
        description="Updated template description",
        tags=["updated", "test"]
    )


class TestTemplateService:
    """Test cases for TemplateService."""
    
    @pytest.mark.asyncio
    async def test_create_template_success(self, mock_uow, mock_template_repository, template_create_data):
        """Test successful template creation."""
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.create(template_create_data)
            
            assert result is not None
            assert result.name == "Research Template"
            assert result.template_type == "task"
            mock_template_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_template_validation_error(self, mock_uow, mock_template_repository):
        """Test template creation with invalid data."""
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            # Test with invalid data (invalid template type)
            invalid_data = TemplateCreate(
                name="Invalid Template",
                description="Test",
                template_type="invalid_type",  # Invalid type
                content={},
                tags=[],
                is_public=False
            )
            
            with pytest.raises(ValueError, match="Invalid template type"):
                await service.create(invalid_data)
    
    @pytest.mark.asyncio
    async def test_get_template_by_id(self, mock_uow, mock_template_repository):
        """Test getting a template by ID."""
        template_id = uuid.uuid4()
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.get(template_id)
            
            assert result is not None
            assert result.name == "Research Template"
            mock_template_repository.get.assert_called_once_with(template_id)
    
    @pytest.mark.asyncio
    async def test_get_template_not_found(self, mock_uow, mock_template_repository):
        """Test getting a non-existent template."""
        template_id = uuid.uuid4()
        mock_template_repository.get.return_value = None
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.get(template_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_template_success(self, mock_uow, mock_template_repository, template_update_data):
        """Test successful template update."""
        template_id = uuid.uuid4()
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.update(template_id, template_update_data)
            
            assert result is not None
            mock_template_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_template_success(self, mock_uow, mock_template_repository):
        """Test successful template deletion."""
        template_id = uuid.uuid4()
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.delete(template_id)
            
            assert result is True
            mock_template_repository.delete.assert_called_once_with(template_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_templates(self, mock_uow, mock_template_repository):
        """Test listing all templates."""
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].name == "Research Template"
            mock_template_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_templates(self, mock_uow, mock_template_repository):
        """Test searching templates."""
        search_query = "research"
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.search(search_query)
            
            assert len(result) == 1
            assert result[0].name == "Research Template"
            mock_template_repository.search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_get_templates_by_type(self, mock_uow, mock_template_repository):
        """Test getting templates by type."""
        template_type = "task"
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.get_by_type(template_type)
            
            assert len(result) == 1
            assert result[0].template_type == "task"
            mock_template_repository.get_by_type.assert_called_once_with(template_type)
    
    @pytest.mark.asyncio
    async def test_get_templates_by_tags(self, mock_uow, mock_template_repository):
        """Test getting templates by tags."""
        tags = ["research", "analysis"]
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.get_by_tags(tags)
            
            assert len(result) == 1
            mock_template_repository.get_by_tags.assert_called_once_with(tags)
    
    @pytest.mark.asyncio
    async def test_validate_template_content(self, mock_uow):
        """Test validation of template content."""
        service = TemplateService(mock_uow)
        
        # Test valid crew template content
        valid_crew_content = {
            "agents": [
                {
                    "name": "agent1",
                    "role": "Agent Role",
                    "goal": "Agent goal",
                    "backstory": "Agent backstory"
                }
            ],
            "tasks": [
                {
                    "name": "task1",
                    "description": "Task description",
                    "agent": "agent1",
                    "expected_output": "Expected output"
                }
            ]
        }
        
        service._validate_template_content("crew", valid_crew_content)  # Should not raise
        
        # Test invalid content (missing required fields)
        invalid_content = {
            "agents": [
                {
                    "name": "agent1"
                    # Missing role, goal, backstory
                }
            ]
        }
        
        with pytest.raises(ValueError, match="Invalid template content"):
            service._validate_template_content("crew", invalid_content)
    
    @pytest.mark.asyncio
    async def test_validate_task_template_content(self, mock_uow):
        """Test validation of task template content."""
        service = TemplateService(mock_uow)
        
        # Test valid task template content
        valid_task_content = {
            "name": "template_task",
            "description": "Task description",
            "agent": "assigned_agent",
            "expected_output": "Expected output",
            "tools": ["tool1", "tool2"]
        }
        
        service._validate_template_content("task", valid_task_content)  # Should not raise
        
        # Test invalid task content
        invalid_task_content = {
            "name": "template_task"
            # Missing description, agent, expected_output
        }
        
        with pytest.raises(ValueError, match="Invalid template content"):
            service._validate_template_content("task", invalid_task_content)
    
    @pytest.mark.asyncio
    async def test_validate_agent_template_content(self, mock_uow):
        """Test validation of agent template content."""
        service = TemplateService(mock_uow)
        
        # Test valid agent template content
        valid_agent_content = {
            "name": "template_agent",
            "role": "Agent Role",
            "goal": "Agent goal",
            "backstory": "Agent backstory",
            "tools": ["tool1"],
            "llm": "gpt-4o-mini"
        }
        
        service._validate_template_content("agent", valid_agent_content)  # Should not raise
        
        # Test invalid agent content
        invalid_agent_content = {
            "name": "template_agent"
            # Missing role, goal, backstory
        }
        
        with pytest.raises(ValueError, match="Invalid template content"):
            service._validate_template_content("agent", invalid_agent_content)
    
    @pytest.mark.asyncio
    async def test_apply_template_to_crew(self, mock_uow, mock_template_repository):
        """Test applying template to create a crew."""
        template_id = uuid.uuid4()
        application_data = {
            "name": "Applied Crew",
            "description": "Crew from template"
        }
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository), \
             patch("src.services.template_service.CrewService") as mock_crew_service:
            
            mock_crew_instance = AsyncMock()
            mock_crew_instance.create_from_template.return_value = {"id": "crew-123"}
            mock_crew_service.return_value = mock_crew_instance
            
            service = TemplateService(mock_uow)
            
            result = await service.apply_template(template_id, application_data)
            
            assert result is not None
            assert "id" in result
            mock_crew_instance.create_from_template.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_clone_template(self, mock_uow, mock_template_repository):
        """Test template cloning."""
        template_id = uuid.uuid4()
        new_name = "Cloned Template"
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            cloned_template = await service.clone_template(template_id, new_name)
            
            assert cloned_template is not None
            mock_template_repository.get.assert_called_once_with(template_id)
            mock_template_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_template_versioning(self, mock_uow, mock_template_repository):
        """Test template versioning."""
        template_id = uuid.uuid4()
        version_data = {
            "version": "2.0",
            "changes": "Updated agents and tasks"
        }
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.create_version(template_id, version_data)
            
            assert result is not None
            mock_template_repository.get.assert_called_once_with(template_id)
            mock_template_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_template_export_import(self, mock_uow, mock_template_repository):
        """Test template export and import."""
        template_id = uuid.uuid4()
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            # Test export
            export_data = await service.export_template(template_id)
            
            assert export_data is not None
            assert "name" in export_data
            assert "template_type" in export_data
            assert "content" in export_data
            
            # Test import
            imported_template = await service.import_template(export_data)
            
            assert imported_template is not None
            mock_template_repository.create.assert_called()
    
    @pytest.mark.asyncio
    async def test_template_metrics(self, mock_uow, mock_template_repository):
        """Test template metrics collection."""
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            metrics = await service.get_template_metrics()
            
            assert "total_templates" in metrics
            assert "templates_by_type" in metrics
            assert "public_templates" in metrics
            assert "popular_tags" in metrics
            assert metrics["total_templates"] >= 0
    
    @pytest.mark.asyncio
    async def test_template_usage_tracking(self, mock_uow, mock_template_repository):
        """Test template usage tracking."""
        template_id = uuid.uuid4()
        usage_data = {
            "application_count": 5,
            "success_rate": 0.9,
            "user_feedback": 4.5
        }
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            result = await service.update_usage_metrics(template_id, usage_data)
            
            assert result is not None
            mock_template_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_template_validation_comprehensive(self, mock_uow):
        """Test comprehensive template validation."""
        service = TemplateService(mock_uow)
        
        # Test complete template validation
        template_data = {
            "name": "Valid Template",
            "description": "A valid template",
            "template_type": "crew",
            "content": {
                "agents": [
                    {
                        "name": "agent1",
                        "role": "Agent Role",
                        "goal": "Agent goal",
                        "backstory": "Agent backstory"
                    }
                ],
                "tasks": [
                    {
                        "name": "task1",
                        "description": "Task description",
                        "agent": "agent1",
                        "expected_output": "Expected output"
                    }
                ]
            },
            "tags": ["test"],
            "is_public": False
        }
        
        # Should not raise exception
        service._validate_template(template_data)
        
        # Test template with missing required fields
        incomplete_template = {
            "name": "Incomplete Template"
            # Missing description, template_type, content
        }
        
        with pytest.raises(ValueError, match="Missing required field"):
            service._validate_template(incomplete_template)
    
    @pytest.mark.asyncio
    async def test_template_dependency_resolution(self, mock_uow):
        """Test template dependency resolution."""
        service = TemplateService(mock_uow)
        
        # Test template with dependencies
        template_content = {
            "agents": [
                {
                    "name": "agent1",
                    "role": "Primary Agent",
                    "goal": "Primary goal",
                    "backstory": "Primary backstory"
                }
            ],
            "tasks": [
                {
                    "name": "task1",
                    "description": "First task",
                    "agent": "agent1",
                    "expected_output": "Output 1"
                },
                {
                    "name": "task2",
                    "description": "Second task",
                    "agent": "agent1",
                    "expected_output": "Output 2",
                    "context": ["task1"]  # Depends on task1
                }
            ]
        }
        
        # Should resolve dependencies correctly
        resolved = service._resolve_template_dependencies(template_content)
        
        assert resolved is not None
        assert len(resolved["tasks"]) == 2
        # Task order should be resolved based on dependencies
    
    @pytest.mark.asyncio
    async def test_duplicate_template_name(self, mock_uow, mock_template_repository, template_create_data):
        """Test creating template with duplicate name."""
        # Mock repository to return existing template with same name
        mock_template_repository.get_by_name.return_value = MagicMock()
        mock_template_repository.create.side_effect = ValueError("Template name already exists")
        
        with patch("src.services.template_service.TemplateRepository", return_value=mock_template_repository):
            service = TemplateService(mock_uow)
            
            with pytest.raises(ValueError, match="Template name already exists"):
                await service.create(template_create_data)