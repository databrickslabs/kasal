"""
Unit tests for Databricks notebook exporter.
"""

import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock

from src.engines.crewai.exporters.databricks_notebook_exporter import DatabricksNotebookExporter


class TestDatabricksNotebookExporter:
    """Tests for DatabricksNotebookExporter class."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

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
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

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
    async def test_export_returns_notebook_structure(self, exporter, sample_crew_data):
        """Test that export returns proper notebook structure."""
        result = await exporter.export(sample_crew_data, {})

        assert 'crew_id' in result
        assert 'crew_name' in result
        assert 'export_format' in result
        assert 'notebook' in result
        assert 'notebook_content' in result
        assert 'metadata' in result
        assert 'generated_at' in result

    @pytest.mark.asyncio
    async def test_export_format_is_databricks_notebook(self, exporter, sample_crew_data):
        """Test that export format is databricks_notebook."""
        result = await exporter.export(sample_crew_data, {})

        assert result['export_format'] == 'databricks_notebook'

    @pytest.mark.asyncio
    async def test_export_notebook_is_valid_json(self, exporter, sample_crew_data):
        """Test that notebook content is valid JSON."""
        result = await exporter.export(sample_crew_data, {})

        # Should be able to parse notebook_content as JSON
        parsed = json.loads(result['notebook_content'])
        assert 'cells' in parsed
        assert 'metadata' in parsed
        assert 'nbformat' in parsed

    @pytest.mark.asyncio
    async def test_export_notebook_has_cells(self, exporter, sample_crew_data):
        """Test that notebook has cells."""
        result = await exporter.export(sample_crew_data, {})

        notebook = result['notebook']
        assert 'cells' in notebook
        assert len(notebook['cells']) > 0

    @pytest.mark.asyncio
    async def test_export_with_tracing_enabled(self, exporter, sample_crew_data):
        """Test export with MLflow tracing enabled."""
        options = {'include_tracing': True}
        result = await exporter.export(sample_crew_data, options)

        notebook_content = result['notebook_content']
        assert 'mlflow' in notebook_content.lower()

    @pytest.mark.asyncio
    async def test_export_with_tracing_disabled(self, exporter, sample_crew_data):
        """Test export with MLflow tracing disabled."""
        options = {'include_tracing': False, 'include_evaluation': False, 'include_deployment': False}
        result = await exporter.export(sample_crew_data, options)

        # Notebook should still be valid but without tracing-specific cells
        assert result['notebook'] is not None

    @pytest.mark.asyncio
    async def test_export_with_evaluation_enabled(self, exporter, sample_crew_data):
        """Test export with evaluation enabled."""
        options = {'include_evaluation': True}
        result = await exporter.export(sample_crew_data, options)

        notebook_content = result['notebook_content']
        # Should contain evaluation-related content
        assert 'eval' in notebook_content.lower() or 'mlflow' in notebook_content.lower()

    @pytest.mark.asyncio
    async def test_export_with_deployment_enabled(self, exporter, sample_crew_data):
        """Test export with deployment enabled."""
        options = {'include_deployment': True}
        result = await exporter.export(sample_crew_data, options)

        notebook_content = result['notebook_content']
        # Should contain deployment-related content
        assert 'deploy' in notebook_content.lower() or 'endpoint' in notebook_content.lower()

    @pytest.mark.asyncio
    async def test_export_metadata_contains_counts(self, exporter, sample_crew_data):
        """Test that metadata contains counts."""
        result = await exporter.export(sample_crew_data, {})

        metadata = result['metadata']
        assert 'agents_count' in metadata
        assert 'tasks_count' in metadata
        assert 'cells_count' in metadata

    @pytest.mark.asyncio
    async def test_export_with_custom_tools(self, exporter):
        """Test export with custom tools."""
        crew_data = {
            'id': 'test-id',
            'name': 'Test',
            'agents': [
                {
                    'name': 'Agent',
                    'role': 'Role',
                    'goal': 'Goal',
                    'backstory': 'Story',
                    'llm': 'databricks-llama-4-maverick',
                    'tools': ['PerplexityTool'],
                }
            ],
            'tasks': [
                {
                    'name': 'Task',
                    'description': 'Desc',
                    'expected_output': 'Output',
                    'agent_id': 'agent-1',
                    'tools': ['PerplexityTool'],
                }
            ],
        }

        options = {'include_custom_tools': True}
        result = await exporter.export(crew_data, options)

        assert result['metadata']['tools_count'] > 0


class TestCreateMarkdownCell:
    """Tests for _create_markdown_cell method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_create_markdown_cell_structure(self, exporter):
        """Test markdown cell has correct structure."""
        cell = exporter._create_markdown_cell("# Title")

        assert cell['cell_type'] == 'markdown'
        assert 'metadata' in cell
        assert 'source' in cell

    def test_create_markdown_cell_source_is_list(self, exporter):
        """Test that source is a list of lines."""
        cell = exporter._create_markdown_cell("Line 1\nLine 2")

        assert isinstance(cell['source'], list)

    def test_create_markdown_cell_with_databricks_metadata(self, exporter):
        """Test that Databricks-specific metadata is included."""
        cell = exporter._create_markdown_cell("Content")

        metadata = cell['metadata']
        assert 'application/vnd.databricks.v1+cell' in metadata


class TestCreateCodeCell:
    """Tests for _create_code_cell method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_create_code_cell_structure(self, exporter):
        """Test code cell has correct structure."""
        cell = exporter._create_code_cell("print('hello')")

        assert cell['cell_type'] == 'code'
        assert 'execution_count' in cell
        assert 'outputs' in cell
        assert 'source' in cell

    def test_create_code_cell_execution_count_is_none(self, exporter):
        """Test that execution_count is None."""
        cell = exporter._create_code_cell("code")

        assert cell['execution_count'] is None

    def test_create_code_cell_outputs_is_empty_list(self, exporter):
        """Test that outputs is an empty list."""
        cell = exporter._create_code_cell("code")

        assert cell['outputs'] == []

    def test_create_code_cell_with_databricks_metadata(self, exporter):
        """Test that Databricks-specific metadata is included."""
        cell = exporter._create_code_cell("code")

        metadata = cell['metadata']
        assert 'application/vnd.databricks.v1+cell' in metadata


class TestGenerateTitleMarkdown:
    """Tests for _generate_title_markdown method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_title_includes_crew_name(self, exporter):
        """Test that title includes crew name."""
        agents = [{'name': 'Agent1'}]
        tasks = [{'name': 'Task1'}]

        result = exporter._generate_title_markdown('My Crew', agents, tasks)

        assert 'My Crew' in result

    def test_generate_title_includes_counts(self, exporter):
        """Test that title includes agent and task counts."""
        agents = [{'name': 'Agent1'}, {'name': 'Agent2'}]
        tasks = [{'name': 'Task1'}]

        result = exporter._generate_title_markdown('Test', agents, tasks)

        assert '2' in result  # 2 agents
        assert '1' in result  # 1 task

    def test_generate_title_includes_agent_names(self, exporter):
        """Test that title includes agent names."""
        agents = [{'name': 'Research Agent'}]
        tasks = []

        result = exporter._generate_title_markdown('Test', agents, tasks)

        assert 'Research Agent' in result


class TestGenerateInstallCode:
    """Tests for _generate_install_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_install_includes_crewai(self, exporter):
        """Test that install code includes crewai."""
        result = exporter._generate_install_code([])

        assert 'crewai' in result.lower()

    def test_generate_install_includes_mlflow(self, exporter):
        """Test that install code includes mlflow."""
        result = exporter._generate_install_code([])

        assert 'mlflow' in result.lower()

    def test_generate_install_includes_pip_magic(self, exporter):
        """Test that install code uses %pip magic."""
        result = exporter._generate_install_code([])

        assert '%pip' in result


class TestGenerateImportsCode:
    """Tests for _generate_imports_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_imports_includes_crewai(self, exporter):
        """Test that imports include crewai."""
        result = exporter._generate_imports_code()

        assert 'from crewai import' in result

    def test_generate_imports_includes_mlflow(self, exporter):
        """Test that imports include mlflow."""
        result = exporter._generate_imports_code()

        assert 'import mlflow' in result

    def test_generate_imports_includes_yaml(self, exporter):
        """Test that imports include yaml."""
        result = exporter._generate_imports_code()

        assert 'import yaml' in result


class TestGenerateEnvironmentConfig:
    """Tests for _generate_environment_config method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_env_config_includes_databricks(self, exporter):
        """Test that env config includes Databricks variables."""
        result = exporter._generate_environment_config()

        assert 'DATABRICKS_HOST' in result
        assert 'DATABRICKS_TOKEN' in result

    def test_generate_env_config_includes_secrets(self, exporter):
        """Test that env config includes secrets usage."""
        result = exporter._generate_environment_config()

        assert 'secrets' in result.lower()

    def test_generate_env_config_includes_warning(self, exporter):
        """Test that env config includes security warning."""
        result = exporter._generate_environment_config()

        assert 'secret_scope' in result or 'SECRET' in result


class TestGenerateAgentsYamlCode:
    """Tests for _generate_agents_yaml_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_agents_yaml_code_includes_yaml(self, exporter):
        """Test that code includes YAML content."""
        agents_yaml = "agent1:\n  role: Researcher"
        result = exporter._generate_agents_yaml_code(agents_yaml)

        assert 'agents_yaml' in result
        assert 'Researcher' in result

    def test_generate_agents_yaml_code_parses_yaml(self, exporter):
        """Test that code includes yaml.safe_load."""
        agents_yaml = "agent1:\n  role: Researcher"
        result = exporter._generate_agents_yaml_code(agents_yaml)

        assert 'yaml.safe_load' in result


class TestGenerateTasksYamlCode:
    """Tests for _generate_tasks_yaml_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_tasks_yaml_code_includes_yaml(self, exporter):
        """Test that code includes YAML content."""
        tasks_yaml = "task1:\n  description: Do something"
        result = exporter._generate_tasks_yaml_code(tasks_yaml)

        assert 'tasks_yaml' in result
        assert 'Do something' in result

    def test_generate_tasks_yaml_code_parses_yaml(self, exporter):
        """Test that code includes yaml.safe_load."""
        tasks_yaml = "task1:\n  description: Do something"
        result = exporter._generate_tasks_yaml_code(tasks_yaml)

        assert 'yaml.safe_load' in result


class TestGenerateMlflowConfig:
    """Tests for _generate_mlflow_config method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_mlflow_config_includes_autolog(self, exporter):
        """Test that MLflow config includes autolog."""
        result = exporter._generate_mlflow_config()

        assert 'autolog' in result


class TestGenerateEvaluationCode:
    """Tests for _generate_evaluation_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    def test_generate_evaluation_code_includes_mlflow(self, exporter):
        """Test that evaluation code includes MLflow."""
        result = exporter._generate_evaluation_code('test_crew')

        assert 'mlflow' in result.lower()

    def test_generate_evaluation_code_includes_metrics(self, exporter):
        """Test that evaluation code includes metrics."""
        result = exporter._generate_evaluation_code('test_crew')

        assert 'metric' in result.lower() or 'eval' in result.lower()


class TestGenerateDeploymentCode:
    """Tests for _generate_deployment_code method."""

    @pytest.fixture
    def exporter(self):
        """Create a DatabricksNotebookExporter instance."""
        return DatabricksNotebookExporter()

    @pytest.fixture
    def sample_agents(self):
        """Create sample agents data."""
        return [
            {
                'name': 'Test Agent',
                'role': 'Researcher',
                'goal': 'Research topics',
                'backstory': 'Expert researcher',
                'llm': 'databricks-llama-4-maverick',
            }
        ]

    @pytest.fixture
    def sample_tasks(self):
        """Create sample tasks data."""
        return [
            {
                'name': 'Test Task',
                'description': 'Test description',
                'expected_output': 'Test output',
                'agent_id': 'test-agent-id',
            }
        ]

    @pytest.mark.asyncio
    async def test_generate_deployment_code_includes_unity_catalog(self, exporter, sample_agents, sample_tasks):
        """Test that deployment code includes Unity Catalog."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        assert 'catalog' in result.lower() or 'unity' in result.lower()

    @pytest.mark.asyncio
    async def test_generate_deployment_code_includes_mlflow(self, exporter, sample_agents, sample_tasks):
        """Test that deployment code includes MLflow."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        assert 'mlflow' in result.lower()

    @pytest.mark.asyncio
    async def test_generate_deployment_code_includes_responses_agent(self, exporter, sample_agents, sample_tasks):
        """Test that deployment code includes ResponsesAgent."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        assert 'ResponsesAgent' in result or 'responses' in result.lower()

    @pytest.mark.asyncio
    async def test_generate_deployment_code_embeds_yaml(self, exporter, sample_agents, sample_tasks):
        """Test that deployment code embeds agents_yaml and tasks_yaml."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        # Should define agents_yaml and tasks_yaml directly in the cell (using single quotes)
        assert "agents_yaml = '" in result
        assert "tasks_yaml = '" in result

    @pytest.mark.asyncio
    async def test_generate_deployment_code_includes_validation_mode_bypass(self, exporter, sample_agents, sample_tasks):
        """Test that deployment code includes validation mode bypass to prevent kernel crash."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        # Should set MLFLOW_VALIDATION_MODE env var before log_model
        assert "MLFLOW_VALIDATION_MODE" in result
        # Should check validation mode in predict method
        assert "os.environ.get('MLFLOW_VALIDATION_MODE')" in result
        # Should clear validation mode after log_model
        assert "os.environ.pop('MLFLOW_VALIDATION_MODE'" in result

    @pytest.mark.asyncio
    async def test_generate_deployment_code_includes_os_import(self, exporter, sample_agents, sample_tasks):
        """Test that deployment code includes os import for environment variable access."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        # The agent Python file must import os for the validation mode check to work
        assert 'import os' in result

    @pytest.mark.asyncio
    async def test_generate_deployment_code_predict_returns_mock_in_validation(self, exporter, sample_agents, sample_tasks):
        """Test that predict method returns mock response during validation."""
        result = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)

        # Should return validation response without executing crew
        assert 'Validation response' in result or 'validation' in result.lower()
        # Should have validation mode check before crew execution
        assert "MLFLOW_VALIDATION_MODE" in result


class TestExportProducesNonBrokenNotebook:
    """Regression tests ensuring the exporter never ships a broken notebook.

    See the broken/fixed notebook pair that motivated these:
    - BaseTool was imported from the wrong module (crewai_tools instead of crewai.tools)
    - the deployment cell used `from databricks import agents` but databricks-agents
      was never installed
    """

    @pytest.fixture
    def exporter(self):
        return DatabricksNotebookExporter()

    @pytest.fixture
    def sample_agents(self):
        return [
            {
                'name': 'Test Agent',
                'role': 'Researcher',
                'goal': 'Research topics',
                'backstory': 'Expert researcher',
                'llm': 'databricks-llama-4-maverick',
            }
        ]

    @pytest.fixture
    def sample_tasks(self):
        return [
            {
                'name': 'Test Task',
                'description': 'Test description',
                'expected_output': 'Test output',
                'agent_id': 'test-agent-id',
            }
        ]

    @pytest.fixture
    def sample_crew_data(self):
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
    async def test_custom_tools_placeholder_imports_basetool_from_crewai_tools(self, exporter):
        """BaseTool must be imported from crewai.tools, not crewai_tools (broken)."""
        result = await exporter._generate_custom_tools_placeholder(['AgentBricksTool'])
        assert 'from crewai.tools import BaseTool' in result
        assert 'from crewai_tools import BaseTool' not in result

    def test_install_code_includes_databricks_agents(self, exporter):
        """Deployment cell uses `from databricks import agents`; it must be installed."""
        result = exporter._generate_install_code([])
        assert 'databricks-agents' in result

    @pytest.mark.asyncio
    async def test_deployment_import_is_satisfied_by_install_cell(self, exporter, sample_agents, sample_tasks):
        """Every `databricks import` in the deploy cell must have an install line."""
        deploy = await exporter._generate_deployment_code('test_crew', [], sample_agents, sample_tasks)
        if 'from databricks import agents' in deploy:
            assert 'databricks-agents' in exporter._generate_install_code([])

    @pytest.mark.asyncio
    async def test_all_generated_code_cells_compile(self, exporter, sample_crew_data):
        """Full export: every code cell must be syntactically valid Python."""
        import ast
        result = await exporter.export(
            sample_crew_data,
            {'include_evaluation': True, 'include_deployment': True, 'include_tracing': True},
        )
        for index, cell in enumerate(result['notebook']['cells']):
            if cell.get('cell_type') != 'code':
                continue
            raw = ''.join(cell['source'])
            python_src = '\n'.join(
                line for line in raw.splitlines()
                if not line.lstrip().startswith(('%', '!'))
            )
            if not python_src.strip():
                continue
            try:
                ast.parse(python_src)
            except SyntaxError as exc:
                pytest.fail(f"Code cell {index} is broken Python: {exc.msg}\n{raw}")

    def test_validate_code_cells_logs_on_broken_cell(self, exporter):
        """The validation safety net logs an error for a syntactically broken cell."""
        broken = [exporter._create_code_cell("def oops(:\n    pass")]
        with patch('src.engines.crewai.exporters.databricks_notebook_exporter.logger') as mock_logger:
            exporter._validate_code_cells(broken, 'test_crew')
            assert mock_logger.error.called

    def test_validate_code_cells_ignores_magics(self, exporter):
        """Notebook magics (%pip) and shell escapes (!) must not trip validation."""
        cell = exporter._create_code_cell("%pip install crewai\n!ls -la\nimport os\nprint(os.getcwd())")
        with patch('src.engines.crewai.exporters.databricks_notebook_exporter.logger') as mock_logger:
            exporter._validate_code_cells([cell], 'test_crew')
            assert not mock_logger.error.called

    def _codex_crew(self, model):
        return {
            'id': 'x', 'name': 'Codex Crew',
            'agents': [{'id': 'a1', 'name': 'Gen', 'role': 'R', 'goal': 'g',
                        'backstory': 'b', 'llm': model, 'tools': []}],
            'tasks': [{'id': 't1', 'name': 'Do', 'description': 'd',
                       'expected_output': 'o', 'agent_id': 'a1'}],
        }

    @pytest.mark.asyncio
    async def test_codex_model_uses_responses_api(self, exporter):
        """gpt-5-3-codex must be built via the Responses API, not chat completions."""
        r = await exporter.export(self._codex_crew('databricks-gpt-5-3-codex'),
                                  {'include_deployment': True})
        crew_cell = next(c for c in r['notebook']['cells']
                         if c['cell_type'] == 'code' and 'make_codex_llm' in ''.join(c['source']))
        src = ''.join(crew_cell['source'])
        assert 'api="responses"' in src
        assert 'OpenAICompletion' in src
        # Must NOT route codex through the plain chat-completions LLM(...)
        assert 'LLM(model="databricks/databricks-gpt-5-3-codex")' not in src

    @pytest.mark.asyncio
    async def test_codex_deployment_wrapper_uses_responses_api(self, exporter):
        """The deployed ResponsesAgent wrapper must also route codex via Responses API."""
        r = await exporter.export(self._codex_crew('databricks-gpt-5-3-codex'),
                                  {'include_deployment': True})
        deploy = next(c for c in r['notebook']['cells']
                      if c['cell_type'] == 'code' and 'CrewAgentWrapper' in ''.join(c['source']))
        src = ''.join(deploy['source'])
        assert "gpt-5-3-codex" in src and "api='responses'" in src
        # Token must fall back to cfg.authenticate() so api_key is never None in serving
        assert 'cfg.authenticate()' in src

    @pytest.mark.asyncio
    async def test_deployment_wrapper_uses_valid_response_status(self, exporter):
        """ResponseOutputMessage only allows in_progress/completed/incomplete — never 'failed'."""
        r = await exporter.export(self._codex_crew('databricks-claude-opus-4-5'),
                                  {'include_deployment': True})
        deploy = next(c for c in r['notebook']['cells']
                      if c['cell_type'] == 'code' and 'CrewAgentWrapper' in ''.join(c['source']))
        src = ''.join(deploy['source'])
        assert "'status': 'failed'" not in src
        assert "'status': 'incomplete'" in src  # error path uses a valid status

    @pytest.mark.asyncio
    async def test_non_codex_model_uses_standard_llm(self, exporter):
        """Anthropic/Claude (and other Databricks models) keep the standard LLM(...) path."""
        r = await exporter.export(self._codex_crew('databricks-claude-opus-4-5'), {})
        crew_cell = next(c for c in r['notebook']['cells']
                         if c['cell_type'] == 'code' and 'Agent(' in ''.join(c['source']))
        src = ''.join(crew_cell['source'])
        assert 'LLM(model="databricks/databricks-claude-opus-4-5")' in src
        assert 'make_codex_llm' not in src
        assert 'OpenAICompletion' not in src

    def _mcp_crew(self, mcp_servers):
        return {
            'id': 'x', 'name': 'MCP Crew',
            'agents': [{'id': 'a1', 'name': 'Gen', 'role': 'R', 'goal': 'g',
                        'backstory': 'b', 'llm': 'databricks-claude-opus-4-5', 'tools': []}],
            'tasks': [{'id': 't1', 'name': 'Do', 'description': 'd',
                       'expected_output': 'o', 'agent_id': 'a1'}],
            'mcp_servers': mcp_servers,
        }

    @pytest.mark.asyncio
    async def test_mcp_databricks_server_wired_into_notebook(self, exporter):
        """A configured Databricks MCP server is imported, parametrized and executed."""
        crew = self._mcp_crew([{
            'name': 'nemotemo',
            'server_url': 'https://ws.cloud.databricks.com/api/2.0/mcp/external/nemotemo',
            'server_type': 'streamable', 'auth_type': 'databricks_spn',
        }])
        r = await exporter.export(crew, {})
        cells = [''.join(c['source']) for c in r['notebook']['cells'] if c['cell_type'] == 'code']
        joined = '\n'.join(cells)
        # Import added
        assert 'from crewai_tools import SerperDevTool, MCPServerAdapter' in joined
        # MCP extra installed so MCPServerAdapter doesn't try to uv-add at runtime
        assert '"crewai-tools[mcp]" mcp' in joined
        # Params built portably from the notebook context (no hardcoded host)
        assert 'mcp_server_params = [' in joined
        assert 'f"{_mcp_host}/api/2.0/mcp/external/nemotemo"' in joined
        assert 'ws.cloud.databricks.com' not in joined  # host not hardcoded
        # Databricks MCP is streamable-HTTP, not SSE (else the adapter times out)
        assert '"transport": "streamable-http"' in joined
        # Crew built via create_crew + executed under the adapter context
        assert 'def create_crew(mcp_tools=None):' in joined
        assert 'with MCPServerAdapter(mcp_server_params) as mcp_tools:' in joined
        assert 'active_crew = create_crew(mcp_tools=mcp_tools)' in joined

    @pytest.mark.asyncio
    async def test_mcp_wired_into_deployment_wrapper(self, exporter):
        """The deployed ResponsesAgent wrapper must also connect to MCP and pass tools."""
        crew = self._mcp_crew([{
            'name': 'nemotemo',
            'server_url': 'https://ws.cloud.databricks.com/api/2.0/mcp/external/nemotemo',
            'server_type': 'streamable', 'auth_type': 'databricks_spn',
        }])
        r = await exporter.export(crew, {'include_deployment': True})
        deploy = next(c for c in r['notebook']['cells']
                      if c['cell_type'] == 'code' and 'CrewAgentWrapper' in ''.join(c['source']))
        src = ''.join(deploy['source'])
        assert 'MCPServerAdapter(_mcp_params)' in src
        assert "transport='streamable-http'" in src
        assert 'tools=mcp_tools' in src           # injected into the agents
        assert '"crewai-tools[mcp]"' in src       # added to pip_requirements

    @pytest.mark.asyncio
    async def test_no_mcp_deployment_wrapper_has_no_adapter(self, exporter):
        """Without MCP the deployed wrapper must not reference MCPServerAdapter."""
        r = await exporter.export(self._codex_crew('databricks-claude-opus-4-5'),
                                  {'include_deployment': True})
        deploy = next(c for c in r['notebook']['cells']
                      if c['cell_type'] == 'code' and 'CrewAgentWrapper' in ''.join(c['source']))
        src = ''.join(deploy['source'])
        assert 'MCPServerAdapter' not in src
        assert 'tools=mcp_tools' not in src

    @pytest.mark.asyncio
    async def test_mcp_third_party_server_uses_env_token(self, exporter):
        """A non-Databricks MCP server keeps its URL and reads a bearer token from env."""
        crew = self._mcp_crew([{
            'name': 'My Tools', 'server_url': 'https://mcp.example.com/sse',
            'server_type': 'sse', 'auth_type': 'api_key',
        }])
        r = await exporter.export(crew, {})
        joined = '\n'.join(''.join(c['source']) for c in r['notebook']['cells'] if c['cell_type'] == 'code')
        assert '"url": "https://mcp.example.com/sse"' in joined
        assert "os.environ.get('MY_TOOLS_MCP_TOKEN'" in joined

    @pytest.mark.asyncio
    async def test_no_mcp_servers_keeps_plain_crew(self, exporter):
        """With no MCP servers the notebook keeps the flat crew (no adapter)."""
        r = await exporter.export(self._mcp_crew([]), {})
        joined = '\n'.join(''.join(c['source']) for c in r['notebook']['cells'] if c['cell_type'] == 'code')
        assert 'MCPServerAdapter' not in joined
        assert 'create_crew' not in joined
        assert 'mcp_server_params' not in joined
        assert 'crew = Crew(' in joined

    @pytest.mark.asyncio
    async def test_deploy_uses_configured_catalog_schema(self, exporter):
        """Deploy cell uses the workspace's configured Databricks catalog/schema."""
        crew = self._codex_crew('databricks-claude-opus-4-5')
        crew['databricks_catalog'] = 'nemotemo_catalog'
        crew['databricks_schema'] = 'kasal'
        r = await exporter.export(crew, {'include_deployment': True})
        joined = '\n'.join(''.join(c['source']) for c in r['notebook']['cells'] if c['cell_type'] == 'code')
        assert 'os.getenv("CATALOG_NAME", "nemotemo_catalog")' in joined
        assert 'os.getenv("SCHEMA_NAME", "kasal")' in joined

    @pytest.mark.asyncio
    async def test_deploy_falls_back_to_default_catalog_schema(self, exporter):
        """Without a configured catalog/schema the deploy cell falls back to main/agents."""
        r = await exporter.export(self._codex_crew('databricks-claude-opus-4-5'),
                                  {'include_deployment': True})
        joined = '\n'.join(''.join(c['source']) for c in r['notebook']['cells'] if c['cell_type'] == 'code')
        assert 'os.getenv("CATALOG_NAME", "main")' in joined
        assert 'os.getenv("SCHEMA_NAME", "agents")' in joined
