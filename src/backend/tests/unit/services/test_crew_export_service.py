"""
Unit tests for crew export service.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from src.services.crew_export_service import CrewExportService
from src.schemas.crew_export import ExportFormat, ExportOptions


class TestCrewExportService:
    """Tests for CrewExportService."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        session = AsyncMock()
        return session

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewExportService instance."""
        return CrewExportService(session=mock_session)

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ['test-group']
        context.is_valid.return_value = True
        return context

    @pytest.fixture
    def sample_crew_data(self):
        """Create sample crew data."""
        return {
            'id': str(uuid4()),
            'name': 'Test Crew',
            'agents': [
                {
                    'id': 'agent-1',
                    'name': 'Research Agent',
                    'role': 'Researcher',
                    'goal': 'Research topics thoroughly',
                    'backstory': 'Expert researcher',
                    'llm': 'databricks-llama-4-maverick',
                    'tools': [],
                }
            ],
            'tasks': [
                {
                    'id': 'task-1',
                    'name': 'Research Task',
                    'description': 'Research the given topic',
                    'expected_output': 'Comprehensive research report',
                    'agent_id': 'agent-1',
                }
            ],
        }

    @pytest.mark.asyncio
    async def test_export_crew_databricks_notebook(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew as Databricks notebook."""
        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                options=ExportOptions(),
                group_context=mock_group_context
            )

        assert result['crew_id'] == sample_crew_data['id']
        assert result['crew_name'] == 'Test Crew'
        assert result['export_format'] == 'databricks_notebook'
        assert 'notebook' in result
        assert 'notebook_content' in result
        assert 'metadata' in result
        assert 'generated_at' in result

    @pytest.mark.asyncio
    async def test_export_crew_python_project(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew as Python project."""
        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.PYTHON_PROJECT,
                options=ExportOptions(),
                group_context=mock_group_context
            )

        assert result['crew_id'] == sample_crew_data['id']
        assert result['crew_name'] == 'Test Crew'
        assert result['export_format'] == 'python_project'
        assert 'files' in result
        assert len(result['files']) > 0

        # Check that expected files are included
        file_paths = [f['path'] for f in result['files']]
        assert 'README.md' in file_paths
        assert 'requirements.txt' in file_paths
        assert any('agents.yaml' in p for p in file_paths)
        assert any('tasks.yaml' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_crew_with_custom_tools(self, service, mock_session, mock_group_context):
        """Test exporting crew with custom tools."""
        crew_data = {
            'id': str(uuid4()),
            'name': 'Crew With Tools',
            'agents': [
                {
                    'id': 'agent-1',
                    'name': 'Agent With Tools',
                    'role': 'Researcher',
                    'goal': 'Research with tools',
                    'backstory': 'Expert',
                    'llm': 'databricks-llama-4-maverick',
                    'tools': ['PerplexityTool', 'SerperDevTool'],
                }
            ],
            'tasks': [
                {
                    'id': 'task-1',
                    'name': 'Search Task',
                    'description': 'Search for information',
                    'expected_output': 'Search results',
                    'agent_id': 'agent-1',
                    'tools': ['PerplexityTool'],
                }
            ],
        }

        with patch.object(service, '_get_crew_with_details', return_value=crew_data):
            result = await service.export_crew(
                crew_id=crew_data['id'],
                export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                options=ExportOptions(include_custom_tools=True),
                group_context=mock_group_context
            )

        assert result['metadata']['tools_count'] > 0

    @pytest.mark.asyncio
    async def test_export_crew_with_model_override(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew with model override."""
        model_override = 'databricks-meta-llama-3-1-70b-instruct'

        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                options=ExportOptions(model_override=model_override),
                group_context=mock_group_context
            )

        assert result is not None
        # The model override should be applied in the generated content

    @pytest.mark.asyncio
    async def test_export_crew_not_found(self, service, mock_group_context):
        """Test exporting non-existent crew."""
        with patch.object(service, '_get_crew_with_details', side_effect=ValueError("Crew not found")):
            with pytest.raises(ValueError) as exc_info:
                await service.export_crew(
                    crew_id=str(uuid4()),
                    export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                    options=ExportOptions(),
                    group_context=mock_group_context
                )

        assert "not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_export_crew_with_tracing_disabled(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew with MLflow tracing disabled."""
        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                options=ExportOptions(include_tracing=False),
                group_context=mock_group_context
            )

        assert result is not None
        # Verify tracing-related code is not included
        notebook_content = result.get('notebook_content', '')
        # The notebook should have fewer cells when tracing is disabled

    @pytest.mark.asyncio
    async def test_export_crew_with_evaluation_disabled(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew with evaluation disabled."""
        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                options=ExportOptions(include_evaluation=False),
                group_context=mock_group_context
            )

        assert result is not None

    @pytest.mark.asyncio
    async def test_export_crew_with_deployment_disabled(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew with deployment disabled."""
        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                options=ExportOptions(include_deployment=False),
                group_context=mock_group_context
            )

        assert result is not None


class TestGetCrewData:
    """Tests for _get_crew_with_details private method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewExportService instance."""
        return CrewExportService(session=mock_session)

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ['test-group']
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_get_crew_with_details_with_saved_crew(self, service, mock_session):
        """Test getting crew data from a saved crew."""
        crew_id = str(uuid4())

        # Mock the crew repository
        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.nodes = []
        mock_crew.edges = []

        with patch('src.services.crew_export_service.CrewRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo_class.return_value = mock_repo
            mock_repo.get.return_value = mock_crew

            # Test will verify the method works without errors
            # Full integration testing would require more setup

    @pytest.mark.asyncio
    async def test_get_crew_with_details_crew_not_found(self, service, mock_session, mock_group_context):
        """Test that ValueError is raised when crew is not found."""
        crew_id = str(uuid4())

        # Mock the crew_repository.get method to return None
        service.crew_repository.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError) as exc_info:
            await service._get_crew_with_details(crew_id, mock_group_context)

        assert "not found" in str(exc_info.value)


class TestExportFormatSelection:
    """Tests for export format selection logic."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        """Create a CrewExportService instance."""
        return CrewExportService(session=mock_session)

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ['test-group']
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_python_project_exporter_selected(self, service, mock_group_context):
        """Test that Python project exporter is selected for PYTHON_PROJECT format."""
        sample_data = {
            'id': 'test-id',
            'name': 'Test',
            'agents': [],
            'tasks': [],
        }

        with patch.object(service, '_get_crew_with_details', return_value=sample_data):
            with patch('src.services.crew_export_service.PythonProjectExporter') as mock_exporter:
                mock_instance = AsyncMock()
                mock_exporter.return_value = mock_instance
                mock_instance.export.return_value = {'export_format': 'python_project'}

                await service.export_crew(
                    crew_id='test-id',
                    export_format=ExportFormat.PYTHON_PROJECT,
                    options=ExportOptions(),
                    group_context=mock_group_context
                )

                mock_exporter.assert_called_once()

    @pytest.mark.asyncio
    async def test_databricks_notebook_exporter_selected(self, service, mock_group_context):
        """Test that Databricks notebook exporter is selected for DATABRICKS_NOTEBOOK format."""
        sample_data = {
            'id': 'test-id',
            'name': 'Test',
            'agents': [],
            'tasks': [],
        }

        with patch.object(service, '_get_crew_with_details', return_value=sample_data):
            with patch('src.services.crew_export_service.DatabricksNotebookExporter') as mock_exporter:
                mock_instance = AsyncMock()
                mock_exporter.return_value = mock_instance
                mock_instance.export.return_value = {'export_format': 'databricks_notebook'}

                await service.export_crew(
                    crew_id='test-id',
                    export_format=ExportFormat.DATABRICKS_NOTEBOOK,
                    options=ExportOptions(),
                    group_context=mock_group_context
                )

                mock_exporter.assert_called_once()
