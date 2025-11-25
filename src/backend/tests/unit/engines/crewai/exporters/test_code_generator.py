"""
Unit tests for code generator.
"""

import pytest

from src.engines.crewai.exporters.code_generator import CodeGenerator


class TestCodeGenerator:
    """Tests for CodeGenerator class."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()


class TestGenerateCrewCode:
    """Tests for generate_crew_code method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    @pytest.fixture
    def sample_agents(self):
        """Create sample agents."""
        return [
            {
                'name': 'Research Agent',
                'role': 'Researcher',
                'goal': 'Research topics',
                'backstory': 'Expert researcher',
                'llm': 'databricks-llama-4-maverick',
                'tools': ['SerperDevTool'],
            }
        ]

    @pytest.fixture
    def sample_tasks(self):
        """Create sample tasks."""
        return [
            {
                'name': 'Research Task',
                'description': 'Research the topic',
                'expected_output': 'Comprehensive report',
                'agent_id': 'research_agent',
            }
        ]

    def test_generate_crew_code_class_based(self, generator, sample_agents, sample_tasks):
        """Test generating class-based crew code."""
        result = generator.generate_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            tools=['SerperDevTool'],
            process_type='sequential',
            include_comments=True,
            for_notebook=False
        )

        assert 'class TestCrewCrew:' in result or '@CrewBase' in result
        assert 'from crewai import' in result
        assert '@agent' in result
        assert '@task' in result
        assert '@crew' in result

    def test_generate_crew_code_for_notebook(self, generator, sample_agents, sample_tasks):
        """Test generating notebook-friendly crew code."""
        result = generator.generate_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            tools=['SerperDevTool'],
            process_type='sequential',
            include_comments=True,
            for_notebook=True
        )

        # Notebook code should have direct instantiation
        assert 'Agent(' in result or 'agents_config' in result
        assert 'Task(' in result or 'tasks_config' in result
        assert 'Crew(' in result

    def test_generate_crew_code_hierarchical_process(self, generator, sample_agents, sample_tasks):
        """Test generating crew code with hierarchical process."""
        result = generator.generate_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            tools=[],
            process_type='hierarchical',
            include_comments=True,
            for_notebook=False
        )

        assert 'Process.hierarchical' in result

    def test_generate_crew_code_without_comments(self, generator, sample_agents, sample_tasks):
        """Test generating crew code without comments."""
        result = generator.generate_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            tools=[],
            process_type='sequential',
            include_comments=False,
            for_notebook=False
        )

        # Should still have code but fewer/no docstrings
        assert 'class' in result or 'def' in result

    def test_generate_crew_code_class_name_formatting(self, generator, sample_agents, sample_tasks):
        """Test that class name is properly formatted."""
        result = generator.generate_crew_code(
            crew_name='my_test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            tools=[],
            process_type='sequential',
            include_comments=True,
            for_notebook=False
        )

        # Class name should be CamelCase
        assert 'MyTestCrewCrew' in result or 'MyTestCrew' in result


class TestGenerateMainCode:
    """Tests for generate_main_code method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    def test_generate_main_code_standalone(self, generator):
        """Test generating standalone main.py code."""
        result = generator.generate_main_code(
            crew_name='test_crew',
            sample_inputs={'topic': 'AI trends'},
            include_comments=True,
            for_notebook=False,
            include_tracing=False
        )

        assert "if __name__ == '__main__':" in result
        assert 'def main():' in result
        assert 'kickoff' in result
        assert 'topic' in result

    def test_generate_main_code_for_notebook(self, generator):
        """Test generating notebook main code."""
        result = generator.generate_main_code(
            crew_name='test_crew',
            sample_inputs={'topic': 'AI trends'},
            include_comments=True,
            for_notebook=True,
            include_tracing=True
        )

        assert 'def run_crew' in result
        assert 'mlflow' in result.lower() or 'execute' in result.lower()

    def test_generate_main_code_without_tracing(self, generator):
        """Test generating notebook main code without MLflow tracing."""
        result = generator.generate_main_code(
            crew_name='test_crew',
            sample_inputs={'topic': 'AI trends'},
            include_comments=True,
            for_notebook=True,
            include_tracing=False
        )

        assert 'def run_crew' in result
        # Should have simpler execution without MLflow

    def test_generate_main_code_with_multiple_inputs(self, generator):
        """Test generating main code with multiple inputs."""
        result = generator.generate_main_code(
            crew_name='test_crew',
            sample_inputs={'topic': 'AI', 'language': 'English', 'depth': 'detailed'},
            include_comments=True,
            for_notebook=False,
            include_tracing=False
        )

        assert 'topic' in result
        assert 'language' in result
        assert 'depth' in result

    def test_generate_main_code_default_inputs(self, generator):
        """Test generating main code with default inputs."""
        result = generator.generate_main_code(
            crew_name='test_crew',
            sample_inputs=None,
            include_comments=True,
            for_notebook=False,
            include_tracing=False
        )

        # Should use default topic
        assert 'Artificial Intelligence' in result or 'topic' in result


class TestGenerateCrewImports:
    """Tests for _generate_crew_imports method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    def test_generate_imports_basic(self, generator):
        """Test generating basic imports."""
        result = generator._generate_crew_imports([], for_notebook=False)

        assert 'from crewai import Agent, Crew, Task, Process' in result
        assert 'from crewai.project import' in result

    def test_generate_imports_with_tools(self, generator):
        """Test generating imports with tools."""
        result = generator._generate_crew_imports(['SerperDevTool'], for_notebook=False)

        assert 'from crewai_tools import SerperDevTool' in result

    def test_generate_imports_for_notebook(self, generator):
        """Test generating imports for notebook."""
        result = generator._generate_crew_imports([], for_notebook=True)

        assert 'from datetime import datetime' in result


class TestGenerateAgentMethod:
    """Tests for _generate_agent_method method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    def test_generate_agent_method_basic(self, generator):
        """Test generating a basic agent method."""
        result = generator._generate_agent_method(
            agent_name='research_agent',
            agent_tools=[],
            include_comments=True,
            for_notebook=False
        )

        assert '@agent' in result
        assert 'def research_agent(self)' in result
        assert 'return Agent(' in result

    def test_generate_agent_method_with_tools(self, generator):
        """Test generating an agent method with tools."""
        result = generator._generate_agent_method(
            agent_name='research_agent',
            agent_tools=['SerperDevTool'],
            include_comments=True,
            for_notebook=False
        )

        assert 'tools=' in result
        assert 'SerperDevTool()' in result

    def test_generate_agent_method_without_comments(self, generator):
        """Test generating an agent method without comments."""
        result = generator._generate_agent_method(
            agent_name='research_agent',
            agent_tools=[],
            include_comments=False,
            for_notebook=False
        )

        assert '@agent' in result
        assert 'def research_agent(self)' in result


class TestGenerateTaskMethod:
    """Tests for _generate_task_method method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    def test_generate_task_method_basic(self, generator):
        """Test generating a basic task method."""
        result = generator._generate_task_method(
            task_name='research_task',
            include_comments=True,
            for_notebook=False
        )

        assert '@task' in result
        assert 'def research_task(self)' in result
        assert 'return Task(' in result

    def test_generate_task_method_without_comments(self, generator):
        """Test generating a task method without comments."""
        result = generator._generate_task_method(
            task_name='research_task',
            include_comments=False,
            for_notebook=False
        )

        assert '@task' in result
        assert 'def research_task(self)' in result


class TestGenerateCrewMethod:
    """Tests for _generate_crew_method method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    def test_generate_crew_method_sequential(self, generator):
        """Test generating a crew method with sequential process."""
        result = generator._generate_crew_method(
            process='Process.sequential',
            include_comments=True
        )

        assert '@crew' in result
        assert 'def crew(self)' in result
        assert 'return Crew(' in result
        assert 'Process.sequential' in result

    def test_generate_crew_method_hierarchical(self, generator):
        """Test generating a crew method with hierarchical process."""
        result = generator._generate_crew_method(
            process='Process.hierarchical',
            include_comments=True
        )

        assert 'Process.hierarchical' in result


class TestGetToolInstantiation:
    """Tests for _get_tool_instantiation method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    def test_get_perplexity_tool_instantiation(self, generator):
        """Test getting PerplexityTool instantiation."""
        result = generator._get_tool_instantiation('PerplexityTool')
        assert result == 'PerplexitySearchTool()'

    def test_get_serper_tool_instantiation(self, generator):
        """Test getting SerperDevTool instantiation."""
        result = generator._get_tool_instantiation('SerperDevTool')
        assert result == 'SerperDevTool()'

    def test_get_unknown_tool_instantiation(self, generator):
        """Test getting unknown tool instantiation returns None."""
        result = generator._get_tool_instantiation('UnknownTool')
        assert result is None

    def test_get_genie_tool_instantiation(self, generator):
        """Test getting GenieTool instantiation."""
        result = generator._get_tool_instantiation('GenieTool')
        assert result == 'GenieTool()'


class TestGenerateNotebookCrewCode:
    """Tests for _generate_notebook_crew_code method."""

    @pytest.fixture
    def generator(self):
        """Create a CodeGenerator instance."""
        return CodeGenerator()

    @pytest.fixture
    def sample_agents(self):
        """Create sample agents."""
        return [
            {
                'name': 'Research Agent',
                'role': 'Researcher',
                'goal': 'Research topics',
                'backstory': 'Expert',
                'llm': 'databricks-llama-4-maverick',
            }
        ]

    @pytest.fixture
    def sample_tasks(self):
        """Create sample tasks."""
        return [
            {
                'name': 'Research Task',
                'description': 'Research',
                'expected_output': 'Report',
            }
        ]

    def test_generate_notebook_crew_code(self, generator, sample_agents, sample_tasks):
        """Test generating notebook-specific crew code."""
        result = generator._generate_notebook_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            process_type='sequential',
            include_comments=True
        )

        assert 'agents_config' in result
        assert 'tasks_config' in result
        assert 'Crew(' in result
        assert 'Agent(' in result
        assert 'Task(' in result

    def test_generate_notebook_crew_code_with_llm(self, generator, sample_agents, sample_tasks):
        """Test that LLM instances are created for agents."""
        result = generator._generate_notebook_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            process_type='sequential',
            include_comments=True
        )

        assert 'LLM(' in result or 'llm' in result

    def test_generate_notebook_crew_code_hierarchical(self, generator, sample_agents, sample_tasks):
        """Test generating notebook crew code with hierarchical process."""
        result = generator._generate_notebook_crew_code(
            crew_name='test_crew',
            agents=sample_agents,
            tasks=sample_tasks,
            process_type='hierarchical',
            include_comments=True
        )

        assert 'Process.hierarchical' in result
