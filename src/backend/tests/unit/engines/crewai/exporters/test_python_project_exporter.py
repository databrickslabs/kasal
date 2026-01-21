"""
Unit tests for Python project exporter.
"""

import pytest
from unittest.mock import AsyncMock, patch

from src.engines.crewai.exporters.python_project_exporter import PythonProjectExporter


class TestPythonProjectExporter:
    """Tests for PythonProjectExporter class."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    @pytest.fixture
    def sample_crew_data(self):
        """Create sample crew data."""
        return {
            'id': 'test-crew-123',
            'name': 'Test Crew',
            'agents': [
                {
                    'id': 'agent-1',
                    'name': 'Research Agent',
                    'role': 'Senior Researcher',
                    'goal': 'Research topics comprehensively',
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
                    'expected_output': 'Comprehensive report',
                    'agent_id': 'agent-1',
                }
            ],
        }


class TestExport:
    """Tests for export method."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    @pytest.fixture
    def sample_crew_data(self):
        """Create sample crew data."""
        return {
            'id': 'test-crew-123',
            'name': 'Test Crew',
            'agents': [
                {
                    'id': 'agent-1',
                    'name': 'Research Agent',
                    'role': 'Researcher',
                    'goal': 'Research',
                    'backstory': 'Expert',
                    'llm': 'databricks-llama-4-maverick',
                    'tools': [],
                }
            ],
            'tasks': [
                {
                    'id': 'task-1',
                    'name': 'Research Task',
                    'description': 'Research',
                    'expected_output': 'Report',
                    'agent_id': 'agent-1',
                }
            ],
        }

    @pytest.mark.asyncio
    async def test_export_returns_files_list(self, exporter, sample_crew_data):
        """Test that export returns a list of files."""
        result = await exporter.export(sample_crew_data, {})

        assert 'files' in result
        assert isinstance(result['files'], list)
        assert len(result['files']) > 0

    @pytest.mark.asyncio
    async def test_export_format_is_python_project(self, exporter, sample_crew_data):
        """Test that export format is python_project."""
        result = await exporter.export(sample_crew_data, {})

        assert result['export_format'] == 'python_project'

    @pytest.mark.asyncio
    async def test_export_includes_readme(self, exporter, sample_crew_data):
        """Test that export includes README.md."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert 'README.md' in file_paths

    @pytest.mark.asyncio
    async def test_export_includes_requirements(self, exporter, sample_crew_data):
        """Test that export includes requirements.txt."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert 'requirements.txt' in file_paths

    @pytest.mark.asyncio
    async def test_export_includes_env_example(self, exporter, sample_crew_data):
        """Test that export includes .env.example."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert '.env.example' in file_paths

    @pytest.mark.asyncio
    async def test_export_includes_gitignore(self, exporter, sample_crew_data):
        """Test that export includes .gitignore."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert '.gitignore' in file_paths

    @pytest.mark.asyncio
    async def test_export_includes_init_py(self, exporter, sample_crew_data):
        """Test that export includes __init__.py."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('__init__.py' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_includes_agents_yaml(self, exporter, sample_crew_data):
        """Test that export includes agents.yaml."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('agents.yaml' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_includes_tasks_yaml(self, exporter, sample_crew_data):
        """Test that export includes tasks.yaml."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('tasks.yaml' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_includes_crew_py(self, exporter, sample_crew_data):
        """Test that export includes crew.py."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('crew.py' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_includes_main_py(self, exporter, sample_crew_data):
        """Test that export includes main.py."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('main.py' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_includes_tests_when_enabled(self, exporter, sample_crew_data):
        """Test that export includes tests when enabled."""
        options = {'include_tests': True}
        result = await exporter.export(sample_crew_data, options)

        file_paths = [f['path'] for f in result['files']]
        assert any('test_crew.py' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_excludes_tests_when_disabled(self, exporter, sample_crew_data):
        """Test that export excludes tests when disabled."""
        options = {'include_tests': False}
        result = await exporter.export(sample_crew_data, options)

        file_paths = [f['path'] for f in result['files']]
        assert not any('test_crew.py' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_export_metadata_contains_counts(self, exporter, sample_crew_data):
        """Test that metadata contains counts."""
        result = await exporter.export(sample_crew_data, {})

        metadata = result['metadata']
        assert 'agents_count' in metadata
        assert 'tasks_count' in metadata
        assert 'tools_count' in metadata

    @pytest.mark.asyncio
    async def test_export_sanitizes_crew_name(self, exporter):
        """Test that crew name is sanitized."""
        crew_data = {
            'id': 'test-id',
            'name': 'My Test Crew',
            'agents': [],
            'tasks': [],
        }

        result = await exporter.export(crew_data, {})

        assert result['metadata']['sanitized_name'] == 'my_test_crew'


class TestGenerateReadme:
    """Tests for _generate_readme method."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    def test_readme_includes_crew_name(self, exporter):
        """Test that README includes crew name."""
        agents = [{'name': 'Agent1'}]
        tasks = [{'name': 'Task1'}]

        result = exporter._generate_readme('test_crew', 'test_crew', agents, tasks)

        assert 'Test Crew' in result or 'test_crew' in result

    def test_readme_includes_agent_count(self, exporter):
        """Test that README includes agent count."""
        agents = [{'name': 'Agent1'}, {'name': 'Agent2'}]
        tasks = []

        result = exporter._generate_readme('test', 'test', agents, tasks)

        assert '2' in result

    def test_readme_includes_task_count(self, exporter):
        """Test that README includes task count."""
        agents = []
        tasks = [{'name': 'Task1'}, {'name': 'Task2'}, {'name': 'Task3'}]

        result = exporter._generate_readme('test', 'test', agents, tasks)

        assert '3' in result

    def test_readme_includes_setup_instructions(self, exporter):
        """Test that README includes setup instructions."""
        result = exporter._generate_readme('test', 'test', [], [])

        assert 'pip install' in result
        assert 'requirements.txt' in result

    def test_readme_includes_run_command(self, exporter):
        """Test that README includes run command."""
        result = exporter._generate_readme('test', 'test_crew', [], [])

        assert 'python' in result
        assert 'main.py' in result


class TestGenerateRequirements:
    """Tests for _generate_requirements method."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    def test_requirements_includes_crewai(self, exporter):
        """Test that requirements includes crewai."""
        result = exporter._generate_requirements([])

        assert 'crewai' in result

    def test_requirements_includes_crewai_tools(self, exporter):
        """Test that requirements includes crewai-tools."""
        result = exporter._generate_requirements([])

        assert 'crewai-tools' in result

    def test_requirements_includes_pydantic(self, exporter):
        """Test that requirements includes pydantic."""
        result = exporter._generate_requirements([])

        assert 'pydantic' in result

    def test_requirements_includes_dotenv(self, exporter):
        """Test that requirements includes python-dotenv."""
        result = exporter._generate_requirements([])

        assert 'python-dotenv' in result


class TestGenerateEnvExample:
    """Tests for _generate_env_example method."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    def test_env_example_includes_databricks(self, exporter):
        """Test that env example includes Databricks variables."""
        result = exporter._generate_env_example()

        assert 'DATABRICKS_HOST' in result
        assert 'DATABRICKS_TOKEN' in result

    def test_env_example_includes_serper(self, exporter):
        """Test that env example includes Serper API key."""
        result = exporter._generate_env_example()

        assert 'SERPER_API_KEY' in result


class TestGenerateGitignore:
    """Tests for _generate_gitignore method."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    def test_gitignore_includes_python_cache(self, exporter):
        """Test that gitignore includes Python cache."""
        result = exporter._generate_gitignore()

        assert '__pycache__' in result
        assert '*.py[cod]' in result  # Covers .pyc, .pyo, .pyd files

    def test_gitignore_includes_venv(self, exporter):
        """Test that gitignore includes venv."""
        result = exporter._generate_gitignore()

        assert 'venv/' in result

    def test_gitignore_includes_env(self, exporter):
        """Test that gitignore includes .env."""
        result = exporter._generate_gitignore()

        assert '.env' in result

    def test_gitignore_includes_ide(self, exporter):
        """Test that gitignore includes IDE directories."""
        result = exporter._generate_gitignore()

        assert '.vscode/' in result or '.idea/' in result

    def test_gitignore_includes_output(self, exporter):
        """Test that gitignore includes output directory."""
        result = exporter._generate_gitignore()

        assert 'output' in result


class TestGenerateTestCode:
    """Tests for _generate_test_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    def test_test_code_includes_pytest(self, exporter):
        """Test that test code includes pytest."""
        result = exporter._generate_test_code('test_crew')

        assert 'import pytest' in result

    def test_test_code_includes_crew_import(self, exporter):
        """Test that test code includes crew import."""
        result = exporter._generate_test_code('test_crew')

        assert 'from test_crew' in result

    def test_test_code_includes_initialization_test(self, exporter):
        """Test that test code includes initialization test."""
        result = exporter._generate_test_code('test_crew')

        assert 'test_crew_initialization' in result

    def test_test_code_includes_agents_test(self, exporter):
        """Test that test code includes agents test."""
        result = exporter._generate_test_code('test_crew')

        assert 'test_agents_defined' in result

    def test_test_code_includes_tasks_test(self, exporter):
        """Test that test code includes tasks test."""
        result = exporter._generate_test_code('test_crew')

        assert 'test_tasks_defined' in result

    def test_test_code_includes_integration_test(self, exporter):
        """Test that test code includes integration test."""
        result = exporter._generate_test_code('test_crew')

        assert 'test_crew_execution' in result
        assert '@pytest.mark.integration' in result

    def test_test_code_class_name_formatting(self, exporter):
        """Test that class name is properly formatted in tests."""
        result = exporter._generate_test_code('my_test_crew')

        # 'my_test_crew' becomes 'MyTestCrew' which already ends with 'Crew'
        # so no additional 'Crew' suffix is added
        assert 'MyTestCrew' in result


class TestFileStructure:
    """Tests for file structure organization."""

    @pytest.fixture
    def exporter(self):
        """Create a PythonProjectExporter instance."""
        return PythonProjectExporter()

    @pytest.fixture
    def sample_crew_data(self):
        """Create sample crew data."""
        return {
            'id': 'test-id',
            'name': 'Test Crew',
            'agents': [{'name': 'Agent', 'role': 'Role', 'goal': 'Goal', 'backstory': 'Story'}],
            'tasks': [{'name': 'Task', 'description': 'Desc', 'expected_output': 'Output', 'agent_id': 'agent-1'}],
        }

    @pytest.mark.asyncio
    async def test_config_files_in_config_directory(self, exporter, sample_crew_data):
        """Test that config files are in config directory."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('config/agents.yaml' in p for p in file_paths)
        assert any('config/tasks.yaml' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_tests_in_tests_directory(self, exporter, sample_crew_data):
        """Test that test files are in tests directory."""
        options = {'include_tests': True}
        result = await exporter.export(sample_crew_data, options)

        file_paths = [f['path'] for f in result['files']]
        assert any('tests/' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_source_files_in_src_directory(self, exporter, sample_crew_data):
        """Test that source files are in src directory."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert any('src/' in p for p in file_paths)

    @pytest.mark.asyncio
    async def test_output_gitkeep_exists(self, exporter, sample_crew_data):
        """Test that output/.gitkeep exists."""
        result = await exporter.export(sample_crew_data, {})

        file_paths = [f['path'] for f in result['files']]
        assert 'output/.gitkeep' in file_paths
