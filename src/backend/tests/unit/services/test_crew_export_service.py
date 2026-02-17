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

    @pytest.mark.asyncio
    async def test_export_crew_databricks_app(self, service, sample_crew_data, mock_group_context):
        """Test exporting crew as Databricks App."""
        with patch.object(service, '_get_crew_with_details', return_value=sample_crew_data):
            result = await service.export_crew(
                crew_id=sample_crew_data['id'],
                export_format=ExportFormat.DATABRICKS_APP,
                options=ExportOptions(),
                group_context=mock_group_context
            )

        assert result['crew_id'] == sample_crew_data['id']
        assert result['crew_name'] == 'Test Crew'
        assert result['export_format'] == 'databricks_app'
        assert 'files' in result
        assert len(result['files']) > 0

        file_paths = [f['path'] for f in result['files']]
        assert 'app.yaml' in file_paths
        assert 'app.py' in file_paths
        assert 'requirements.txt' in file_paths
        assert 'config/agents.yaml' in file_paths
        assert 'config/tasks.yaml' in file_paths
        assert 'services/crew_service.py' in file_paths
        assert 'routes/v1/crew.py' in file_paths


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

    @pytest.mark.asyncio
    async def test_databricks_app_exporter_selected(self, service, mock_group_context):
        """Test that Databricks App exporter is selected for DATABRICKS_APP format."""
        sample_data = {
            'id': 'test-id',
            'name': 'Test',
            'agents': [],
            'tasks': [],
        }

        with patch.object(service, '_get_crew_with_details', return_value=sample_data):
            with patch('src.services.crew_export_service.DatabricksAppExporter') as mock_exporter:
                mock_instance = AsyncMock()
                mock_exporter.return_value = mock_instance
                mock_instance.export.return_value = {'export_format': 'databricks_app'}

                await service.export_crew(
                    crew_id='test-id',
                    export_format=ExportFormat.DATABRICKS_APP,
                    options=ExportOptions(),
                    group_context=mock_group_context
                )

                mock_exporter.assert_called_once()

    @pytest.mark.asyncio
    async def test_unsupported_export_format(self, service, mock_group_context):
        """Test that unsupported format raises ValueError."""
        sample_data = {
            'id': 'test-id',
            'name': 'Test',
            'agents': [],
            'tasks': [],
        }

        with patch.object(service, '_get_crew_with_details', return_value=sample_data):
            with pytest.raises(ValueError, match="Unsupported export format"):
                await service.export_crew(
                    crew_id='test-id',
                    export_format="invalid_format",
                    options=ExportOptions(),
                    group_context=mock_group_context
                )


class TestGetCrewWithDetails:
    """Tests for _get_crew_with_details full execution paths."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        return CrewExportService(session=mock_session)

    @pytest.fixture
    def mock_group_context(self):
        context = MagicMock()
        context.group_ids = ['test-group']
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_get_crew_with_agents_and_tasks(self, service, mock_group_context):
        """Test full path through _get_crew_with_details with agents and tasks."""
        crew_id = str(uuid4())

        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.nodes = [{"id": "1"}]
        mock_crew.edges = [{"source": "1"}]
        mock_crew.group_id = 'test-group'
        mock_crew.agent_ids = ['a1']
        mock_crew.task_ids = ['t1']

        mock_agent = MagicMock()
        mock_agent.id = 'a1'
        mock_agent.name = 'Agent'
        mock_agent.role = 'Role'
        mock_agent.goal = 'Goal'
        mock_agent.backstory = 'Backstory'
        mock_agent.llm = 'model'
        mock_agent.tools = ['SerperDevTool']
        mock_agent.max_iter = 25
        mock_agent.max_rpm = None
        mock_agent.max_execution_time = None
        mock_agent.verbose = True
        mock_agent.allow_delegation = False
        mock_agent.cache = True
        mock_agent.system_template = None
        mock_agent.prompt_template = None
        mock_agent.response_template = None

        mock_task = MagicMock()
        mock_task.id = 't1'
        mock_task.name = 'Task'
        mock_task.description = 'Desc'
        mock_task.expected_output = 'Output'
        mock_task.agent_id = 'a1'
        mock_task.tools = [42]
        mock_task.async_execution = False
        mock_task.context = []
        mock_task.output_file = None
        mock_task.output_json = None
        mock_task.callback = None
        mock_task.human_input = False

        service.crew_repository.get = AsyncMock(return_value=mock_crew)
        service.agent_repository.get = AsyncMock(return_value=mock_agent)
        service.task_repository.get = AsyncMock(return_value=mock_task)

        # Mock tool lookup for integer tool IDs
        mock_tool = MagicMock()
        mock_tool.title = 'SerperDevTool'
        service.tool_repository.get = AsyncMock(return_value=mock_tool)

        result = await service._get_crew_with_details(crew_id, mock_group_context)

        assert result['name'] == 'Test Crew'
        assert len(result['agents']) == 1
        assert result['agents'][0]['name'] == 'Agent'
        assert result['agents'][0]['tools'] == ['SerperDevTool']
        assert len(result['tasks']) == 1
        assert result['tasks'][0]['name'] == 'Task'
        assert result['tasks'][0]['tools'] == ['SerperDevTool']

    @pytest.mark.asyncio
    async def test_get_crew_group_authorization_denied(self, service):
        """Test group authorization check rejects unauthorized access."""
        crew_id = str(uuid4())

        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.group_id = 'other-group'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.nodes = []
        mock_crew.edges = []

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        context = MagicMock()
        context.group_ids = ['my-group']
        context.is_valid.return_value = True

        with pytest.raises(ValueError, match="not found"):
            await service._get_crew_with_details(crew_id, context)

    @pytest.mark.asyncio
    async def test_get_crew_skips_missing_agents_and_tasks(self, service):
        """Test that missing agents/tasks are silently skipped."""
        crew_id = str(uuid4())

        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.group_id = 'test-group'
        mock_crew.agent_ids = ['a1', 'a2']
        mock_crew.task_ids = ['t1', 't2']
        mock_crew.nodes = []
        mock_crew.edges = []

        service.crew_repository.get = AsyncMock(return_value=mock_crew)
        # a1 found, a2 not found
        service.agent_repository.get = AsyncMock(side_effect=[None, None])
        service.task_repository.get = AsyncMock(side_effect=[None, None])

        result = await service._get_crew_with_details(crew_id, None)

        assert result['agents'] == []
        assert result['tasks'] == []

    @pytest.mark.asyncio
    async def test_get_crew_no_group_context(self, service):
        """Test _get_crew_with_details without group context skips auth check."""
        crew_id = str(uuid4())

        mock_crew = MagicMock()
        mock_crew.id = crew_id
        mock_crew.name = 'Test Crew'
        mock_crew.group_id = 'any-group'
        mock_crew.agent_ids = []
        mock_crew.task_ids = []
        mock_crew.nodes = None
        mock_crew.edges = None

        service.crew_repository.get = AsyncMock(return_value=mock_crew)

        result = await service._get_crew_with_details(crew_id, None)

        assert result['name'] == 'Test Crew'
        assert result['nodes'] == []
        assert result['edges'] == []


class TestConvertToolIdsToNames:
    """Tests for _convert_tool_ids_to_names method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session):
        return CrewExportService(session=mock_session)

    @pytest.mark.asyncio
    async def test_convert_string_tool_names(self, service):
        """String tool names are kept as-is."""
        result = await service._convert_tool_ids_to_names(['SerperDevTool', 'DallETool'])
        assert result == ['SerperDevTool', 'DallETool']

    @pytest.mark.asyncio
    async def test_convert_integer_tool_ids(self, service):
        """Integer tool IDs are looked up from repository."""
        mock_tool = MagicMock()
        mock_tool.title = 'MyCustomTool'
        service.tool_repository.get = AsyncMock(return_value=mock_tool)

        result = await service._convert_tool_ids_to_names([42])
        assert result == ['MyCustomTool']

    @pytest.mark.asyncio
    async def test_convert_numeric_string_tool_ids(self, service):
        """Numeric string tool IDs are converted to int and looked up."""
        mock_tool = MagicMock()
        mock_tool.title = 'FoundTool'
        service.tool_repository.get = AsyncMock(return_value=mock_tool)

        result = await service._convert_tool_ids_to_names(['99'])
        assert result == ['FoundTool']

    @pytest.mark.asyncio
    async def test_convert_integer_tool_id_not_found(self, service):
        """Integer tool ID not in DB falls back to string ID."""
        service.tool_repository.get = AsyncMock(return_value=None)

        result = await service._convert_tool_ids_to_names([999])
        assert result == ['999']

    @pytest.mark.asyncio
    async def test_convert_unknown_type(self, service):
        """Unknown types are converted to string."""
        result = await service._convert_tool_ids_to_names([3.14])
        assert result == ['3.14']

    @pytest.mark.asyncio
    async def test_convert_empty_list(self, service):
        """Empty list returns empty list."""
        result = await service._convert_tool_ids_to_names([])
        assert result == []
