"""
Unit tests for Databricks App exporter.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from io import StringIO

from src.engines.crewai.exporters.databricks_app_exporter import DatabricksAppExporter


class TestDatabricksAppExporter:
    """Tests for DatabricksAppExporter class."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    @pytest.fixture
    def sample_crew_data(self):
        return {
            "id": "test-crew-123",
            "name": "Test Crew",
            "agents": [
                {
                    "id": "agent-1",
                    "name": "Research Agent",
                    "role": "Senior Researcher",
                    "goal": "Research topics comprehensively",
                    "backstory": "Expert researcher with deep knowledge",
                    "llm": "databricks-llama-4-maverick",
                    "tools": [],
                },
                {
                    "id": "agent-2",
                    "name": "Writer Agent",
                    "role": "Technical Writer",
                    "goal": "Write clear technical content",
                    "backstory": "Experienced writer",
                    "llm": "databricks-llama-4-maverick",
                    "tools": ["SerperDevTool"],
                },
            ],
            "tasks": [
                {
                    "id": "task-1",
                    "name": "Research Task",
                    "description": "Research the given topic",
                    "expected_output": "Comprehensive research report",
                    "agent_id": "agent-1",
                    "tools": [],
                },
                {
                    "id": "task-2",
                    "name": "Writing Task",
                    "description": "Write an article based on research",
                    "expected_output": "Well-written article",
                    "agent_id": "agent-2",
                    "tools": [],
                },
            ],
        }

    @pytest.fixture
    def default_options(self):
        return {
            "include_custom_tools": True,
            "include_comments": True,
            "include_static_frontend": True,
            "include_obo_auth": True,
        }


class TestExport:
    """Tests for the export method."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    @pytest.fixture
    def sample_crew_data(self):
        return {
            "id": "test-crew-123",
            "name": "Test Crew",
            "agents": [
                {
                    "id": "agent-1",
                    "name": "Research Agent",
                    "role": "Researcher",
                    "goal": "Research",
                    "backstory": "Expert",
                    "llm": "databricks-llama-4-maverick",
                    "tools": [],
                }
            ],
            "tasks": [
                {
                    "id": "task-1",
                    "name": "Research Task",
                    "description": "Research the given topic",
                    "expected_output": "Report",
                    "agent_id": "agent-1",
                }
            ],
        }

    @pytest.mark.asyncio
    async def test_export_returns_expected_structure(self, exporter, sample_crew_data):
        """Export result has required top-level keys."""
        result = await exporter.export(sample_crew_data, {})

        assert result["crew_id"] == "test-crew-123"
        assert result["crew_name"] == "Test Crew"
        assert result["export_format"] == "databricks_app"
        assert "files" in result
        assert "metadata" in result
        assert "generated_at" in result
        assert "size_bytes" in result

    @pytest.mark.asyncio
    async def test_export_returns_all_core_files(self, exporter, sample_crew_data):
        """Export includes all expected file paths."""
        result = await exporter.export(sample_crew_data, {"include_static_frontend": True})

        paths = {f["path"] for f in result["files"]}

        expected_paths = {
            "app.yaml",
            "app.py",
            "requirements.txt",
            ".env.example",
            "README.md",
            "config/agents.yaml",
            "config/tasks.yaml",
            "services/__init__.py",
            "services/crew_service.py",
            "routes/__init__.py",
            "routes/v1/__init__.py",
            "routes/v1/healthcheck.py",
            "routes/v1/crew.py",
            "models/__init__.py",
            "models/crew.py",
            "tools/__init__.py",
            "static/index.html",
            "static/styles.css",
            "static/app.js",
        }
        assert expected_paths.issubset(paths), f"Missing paths: {expected_paths - paths}"

    @pytest.mark.asyncio
    async def test_export_without_static_frontend(self, exporter, sample_crew_data):
        """Static frontend files are excluded when option is False."""
        result = await exporter.export(
            sample_crew_data, {"include_static_frontend": False}
        )
        paths = {f["path"] for f in result["files"]}

        assert "static/index.html" not in paths
        assert "static/styles.css" not in paths
        assert "static/app.js" not in paths

    @pytest.mark.asyncio
    async def test_export_metadata(self, exporter, sample_crew_data):
        """Metadata contains correct counts."""
        result = await exporter.export(sample_crew_data, {})

        meta = result["metadata"]
        assert meta["agents_count"] == 1
        assert meta["tasks_count"] == 1
        assert meta["sanitized_name"] == "test_crew"

    @pytest.mark.asyncio
    async def test_export_with_model_override(self, exporter, sample_crew_data):
        """Model override is applied to YAML config."""
        result = await exporter.export(
            sample_crew_data, {"model_override": "gpt-4o"}
        )

        agents_yaml = next(
            f for f in result["files"] if f["path"] == "config/agents.yaml"
        )
        assert "gpt-4o" in agents_yaml["content"]

    @pytest.mark.asyncio
    async def test_export_with_custom_tools(self, exporter, sample_crew_data):
        """Custom tools agent triggers tools/custom_tools.py when file found."""
        sample_crew_data["agents"][0]["tools"] = ["PerplexityTool"]

        with patch(
            "src.engines.crewai.exporters.databricks_app_exporter.aiofiles.open",
            side_effect=FileNotFoundError,
        ):
            with patch("pathlib.Path.exists", return_value=False):
                result = await exporter.export(
                    sample_crew_data, {"include_custom_tools": True}
                )

        paths = {f["path"] for f in result["files"]}
        # custom_tools.py should not be present when file not found
        assert "tools/custom_tools.py" not in paths

    @pytest.mark.asyncio
    async def test_export_with_custom_tools_file_found(self, exporter, sample_crew_data):
        """Custom tools file is included when the tool source file exists."""
        sample_crew_data["agents"][0]["tools"] = ["PerplexityTool"]

        mock_file = AsyncMock()
        mock_file.__aenter__ = AsyncMock(return_value=mock_file)
        mock_file.__aexit__ = AsyncMock(return_value=False)
        mock_file.read = AsyncMock(return_value="class PerplexitySearchTool:\n    pass\n")

        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "src.engines.crewai.exporters.databricks_app_exporter.aiofiles.open",
                return_value=mock_file,
            ):
                result = await exporter.export(
                    sample_crew_data, {"include_custom_tools": True}
                )

        paths = {f["path"] for f in result["files"]}
        assert "tools/custom_tools.py" in paths
        custom_tools = next(
            f for f in result["files"] if f["path"] == "tools/custom_tools.py"
        )
        assert "PerplexitySearchTool" in custom_tools["content"]

    @pytest.mark.asyncio
    async def test_export_with_custom_tools_read_error(self, exporter, sample_crew_data):
        """Custom tools file read error is handled gracefully."""
        sample_crew_data["agents"][0]["tools"] = ["PerplexityTool"]

        mock_file = AsyncMock()
        mock_file.__aenter__ = AsyncMock(side_effect=IOError("Read error"))
        mock_file.__aexit__ = AsyncMock(return_value=False)

        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "src.engines.crewai.exporters.databricks_app_exporter.aiofiles.open",
                return_value=mock_file,
            ):
                result = await exporter.export(
                    sample_crew_data, {"include_custom_tools": True}
                )

        paths = {f["path"] for f in result["files"]}
        # Should not be present since the read failed
        assert "tools/custom_tools.py" not in paths


class TestAppYaml:
    """Tests for app.yaml generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_app_yaml_has_uvicorn_command(self, exporter):
        content = exporter._generate_app_yaml()
        assert "uvicorn" in content
        assert "app:app" in content
        assert "0.0.0.0" in content
        assert "8000" in content

    def test_app_yaml_custom_port(self, exporter):
        content = exporter._generate_app_yaml(port=9000)
        assert "9000" in content


class TestRequirements:
    """Tests for requirements.txt generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_requirements_includes_crewai(self, exporter):
        content = exporter._generate_requirements([])
        assert "crewai" in content
        assert "pyyaml" in content
        assert "litellm" in content

    def test_requirements_excludes_fastapi(self, exporter):
        """FastAPI and uvicorn are pre-installed on Databricks Apps."""
        content = exporter._generate_requirements([])
        assert "fastapi" not in content
        assert "uvicorn" not in content

    def test_requirements_adds_tool_deps(self, exporter):
        content = exporter._generate_requirements(["PerplexityTool"])
        assert "requests" in content

    def test_requirements_adds_scrape_deps(self, exporter):
        content = exporter._generate_requirements(["ScrapeWebsiteTool"])
        assert "beautifulsoup4" in content

    def test_requirements_includes_mlflow(self, exporter):
        """Requirements should always include mlflow."""
        content = exporter._generate_requirements([])
        assert "mlflow" in content

    def test_requirements_includes_chromadb(self, exporter):
        """Requirements should always include chromadb for default memory."""
        content = exporter._generate_requirements([])
        assert "chromadb" in content

    def test_requirements_includes_databricks_sdk_for_genie(self, exporter):
        """Requirements should include databricks-sdk when GenieTool is used."""
        content = exporter._generate_requirements(["GenieTool"])
        assert "databricks-sdk" in content

    def test_requirements_includes_databricks_sdk_for_query_tool(self, exporter):
        """Requirements should include databricks-sdk when DatabricksQueryTool is used."""
        content = exporter._generate_requirements(["DatabricksQueryTool"])
        assert "databricks-sdk" in content

    def test_requirements_excludes_databricks_sdk_without_tools(self, exporter):
        """Requirements should not include databricks-sdk when no Databricks tools used."""
        content = exporter._generate_requirements([])
        assert "databricks-sdk" not in content


class TestAppPy:
    """Tests for app.py generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_app_py_includes_fastapi(self, exporter):
        content = exporter._generate_app_py("test_crew", True)
        assert "FastAPI" in content
        assert "api_router" in content

    def test_app_py_with_static(self, exporter):
        content = exporter._generate_app_py("test_crew", True)
        assert "StaticFiles" in content
        assert "FileResponse" in content
        assert "static" in content

    def test_app_py_without_static(self, exporter):
        content = exporter._generate_app_py("test_crew", False)
        assert "StaticFiles" not in content
        assert "FileResponse" not in content

    def test_app_py_includes_mlflow_experiment(self, exporter):
        """App.py lifespan should set up MLflow experiment."""
        content = exporter._generate_app_py("test_crew", True)
        assert "mlflow" in content
        assert "set_experiment" in content
        assert "MLFLOW_EXPERIMENT_NAME" in content
        assert "/Shared/test_crew" in content


class TestCrewService:
    """Tests for crew_service.py generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_crew_service_includes_obo(self, exporter):
        content = exporter._generate_crew_service(
            "test_crew", [], [], [], None, include_obo=True
        )
        assert "user_token" in content
        assert "DATABRICKS_TOKEN" in content

    def test_crew_service_without_obo(self, exporter):
        content = exporter._generate_crew_service(
            "test_crew", [], [], [], None, include_obo=False
        )
        assert "user_token" not in content

    def test_crew_service_includes_mlflow(self, exporter):
        """Generated crew service should include MLflow autolog when tracing enabled."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], [], None,
            include_obo=True, include_tracing=True,
        )
        assert "mlflow.crewai.autolog()" in content
        assert "import mlflow" in content

    def test_crew_service_excludes_mlflow_when_tracing_disabled(self, exporter):
        """Generated crew service should omit MLflow when tracing disabled."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], [], None,
            include_obo=True, include_tracing=False,
        )
        assert "mlflow.crewai.autolog()" not in content

    def test_crew_service_includes_memory(self, exporter):
        """Generated Crew should have memory=True for default CrewAI memory."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], [], None, include_obo=True
        )
        assert "memory=True" in content

    def test_crew_service_includes_tool_map(self, exporter):
        """Generated crew service should include TOOL_MAP with tool entries."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], ["SerperDevTool", "ScrapeWebsiteTool"],
            None, include_obo=True,
        )
        assert "TOOL_MAP" in content
        assert '"SerperDevTool"' in content
        assert '"ScrapeWebsiteTool"' in content

    def test_crew_service_includes_build_tools_method(self, exporter):
        """Generated crew service should include _build_tools method."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], ["SerperDevTool"], None, include_obo=True,
        )
        assert "_build_tools" in content

    def test_crew_service_agents_use_tools(self, exporter):
        """Agent construction should pass tools from config."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], ["SerperDevTool"], None, include_obo=True,
        )
        assert 'agent_tools = self._build_tools(cfg.get("tools", []))' in content
        assert "tools=agent_tools" in content

    def test_crew_service_tasks_use_tools(self, exporter):
        """Task construction should pass tools from config."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], ["SerperDevTool"], None, include_obo=True,
        )
        assert 'task_tools = self._build_tools(cfg.get("tools", []))' in content
        assert "tools=task_tools" in content

    def test_crew_service_get_info_includes_tools(self, exporter):
        """get_info() should return tools list."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], ["SerperDevTool"], None, include_obo=True,
        )
        assert '"tools": list(TOOL_MAP.keys())' in content

    def test_crew_service_genie_tool_uses_env(self, exporter):
        """GenieTool instantiation should read GENIE_SPACE_ID from env."""
        content = exporter._generate_crew_service(
            "test_crew", [], [], ["GenieTool"], None, include_obo=True,
        )
        assert "GENIE_SPACE_ID" in content


class TestEnvExample:
    """Tests for .env.example generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_env_example_includes_mlflow(self, exporter):
        """Env example should include MLflow experiment name."""
        content = exporter._generate_env_example([])
        assert "MLFLOW_EXPERIMENT_NAME" in content

    def test_env_example_includes_genie_space_id(self, exporter):
        """Env example should include GENIE_SPACE_ID when GenieTool used."""
        content = exporter._generate_env_example(["GenieTool"])
        assert "GENIE_SPACE_ID" in content

    def test_env_example_excludes_genie_without_tool(self, exporter):
        """Env example should not include GENIE_SPACE_ID when GenieTool not used."""
        content = exporter._generate_env_example([])
        assert "GENIE_SPACE_ID" not in content


class TestRoutes:
    """Tests for route generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_routes_init_has_api_prefix(self, exporter):
        content = exporter._generate_routes_init()
        assert '/api' in content

    def test_v1_routes_init_has_v1_prefix(self, exporter):
        content = exporter._generate_routes_v1_init()
        assert '/v1' in content
        assert "healthcheck_router" in content
        assert "crew_router" in content

    def test_crew_routes_has_execute(self, exporter):
        content = exporter._generate_crew_routes("test_crew")
        assert "/execute" in content
        assert "/info" in content
        assert "x_forwarded_access_token" in content

    def test_healthcheck_routes(self, exporter):
        content = exporter._generate_healthcheck_routes("test_crew")
        assert "/health" in content
        assert "test_crew" in content


class TestModels:
    """Tests for Pydantic models generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_models_crew(self, exporter):
        content = exporter._generate_models_crew()
        assert "CrewExecuteRequest" in content
        assert "CrewExecuteResponse" in content
        assert "CrewInfoResponse" in content
        assert "BaseModel" in content


class TestStaticFrontend:
    """Tests for static frontend generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_html_includes_crew_name(self, exporter):
        content = exporter._generate_static_html("My Test Crew")
        assert "My Test Crew" in content
        assert "execute-form" in content
        assert "app.js" in content

    def test_css_is_not_empty(self, exporter):
        content = exporter._generate_static_css()
        assert len(content) > 100
        assert "body" in content

    def test_js_calls_api(self, exporter):
        content = exporter._generate_static_js("test_crew")
        assert "/api/v1" in content
        assert "crew/execute" in content
        assert "crew/info" in content


class TestReadme:
    """Tests for README generation."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    def test_readme_includes_agents_and_tasks(self, exporter):
        agents = [{"name": "Agent A", "role": "Tester"}]
        tasks = [{"name": "Task A", "description": "Test something"}]
        content = exporter._generate_readme("test_crew", agents, tasks)
        assert "Agent A" in content
        assert "Task A" in content
        assert "databricks apps deploy" in content
        assert "/api/v1" in content


class TestYAMLToolsIntegration:
    """Tests for tools appearing in generated YAML configs."""

    @pytest.fixture
    def exporter(self):
        return DatabricksAppExporter()

    @pytest.mark.asyncio
    async def test_tools_appear_in_agents_yaml(self, exporter):
        """Tools specified on agents should appear in generated agents.yaml."""
        crew_data = {
            "id": "test-123",
            "name": "Tool Crew",
            "agents": [
                {
                    "id": "a1",
                    "name": "Search Agent",
                    "role": "Searcher",
                    "goal": "Search",
                    "backstory": "Expert searcher",
                    "llm": "databricks-llama-4-maverick",
                    "tools": ["SerperDevTool", "ScrapeWebsiteTool"],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Search Task",
                    "description": "Search the web",
                    "expected_output": "Results",
                    "agent_id": "a1",
                }
            ],
        }
        result = await exporter.export(crew_data, {})
        agents_yaml = next(
            f for f in result["files"] if f["path"] == "config/agents.yaml"
        )
        assert "SerperDevTool" in agents_yaml["content"]
        assert "ScrapeWebsiteTool" in agents_yaml["content"]

    @pytest.mark.asyncio
    async def test_tools_appear_in_tasks_yaml(self, exporter):
        """Tools specified on tasks should appear in generated tasks.yaml."""
        crew_data = {
            "id": "test-123",
            "name": "Tool Crew",
            "agents": [
                {
                    "id": "a1",
                    "name": "Agent",
                    "role": "Worker",
                    "goal": "Work",
                    "backstory": "Worker",
                    "llm": "databricks-llama-4-maverick",
                    "tools": [],
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "Web Task",
                    "description": "Scrape the web",
                    "expected_output": "Data",
                    "agent_id": "a1",
                    "tools": ["ScrapeWebsiteTool"],
                }
            ],
        }
        result = await exporter.export(crew_data, {})
        tasks_yaml = next(
            f for f in result["files"] if f["path"] == "config/tasks.yaml"
        )
        assert "ScrapeWebsiteTool" in tasks_yaml["content"]
