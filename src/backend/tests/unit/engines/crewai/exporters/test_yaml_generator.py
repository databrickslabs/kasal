"""
Unit tests for YAML generator.
"""

import pytest
import yaml

from src.engines.crewai.exporters.yaml_generator import YAMLGenerator


class TestYAMLGenerator:
    """Tests for YAMLGenerator class."""

    @pytest.fixture
    def generator(self):
        """Create a YAMLGenerator instance."""
        return YAMLGenerator()


class TestGenerateAgentsYaml:
    """Tests for generate_agents_yaml method."""

    @pytest.fixture
    def generator(self):
        """Create a YAMLGenerator instance."""
        return YAMLGenerator()

    @pytest.fixture
    def sample_agents(self):
        """Create sample agents."""
        return [
            {
                'name': 'Research Agent',
                'role': 'Senior Researcher',
                'goal': 'Research and analyze topics comprehensively',
                'backstory': 'Expert researcher with years of experience',
                'llm': 'databricks-llama-4-maverick',
            },
            {
                'name': 'Writer Agent',
                'role': 'Content Writer',
                'goal': 'Write engaging content',
                'backstory': 'Professional writer',
                'llm': 'databricks-meta-llama-3-1-70b-instruct',
            },
        ]

    def test_generate_agents_yaml_basic(self, generator, sample_agents):
        """Test generating basic agents YAML."""
        result = generator.generate_agents_yaml(
            agents=sample_agents,
            model_override=None,
            include_comments=True
        )

        # Should be valid YAML
        parsed = yaml.safe_load(result)
        assert 'research_agent' in parsed
        assert 'writer_agent' in parsed

    def test_generate_agents_yaml_content(self, generator, sample_agents):
        """Test generated agents YAML content."""
        result = generator.generate_agents_yaml(
            agents=sample_agents,
            model_override=None,
            include_comments=False
        )

        parsed = yaml.safe_load(result)

        # Check research agent
        assert parsed['research_agent']['role'] == 'Senior Researcher'
        assert parsed['research_agent']['goal'] == 'Research and analyze topics comprehensively'
        assert parsed['research_agent']['llm'] == 'databricks-llama-4-maverick'

    def test_generate_agents_yaml_with_model_override(self, generator, sample_agents):
        """Test generating agents YAML with model override."""
        result = generator.generate_agents_yaml(
            agents=sample_agents,
            model_override='custom-model',
            include_comments=False
        )

        parsed = yaml.safe_load(result)

        # All agents should use the override model
        for agent_name, agent_config in parsed.items():
            assert agent_config['llm'] == 'custom-model'

    def test_generate_agents_yaml_with_comments(self, generator, sample_agents):
        """Test generating agents YAML with comments."""
        result = generator.generate_agents_yaml(
            agents=sample_agents,
            model_override=None,
            include_comments=True
        )

        assert '# Agent Configuration' in result
        assert 'role' in result
        assert 'goal' in result

    def test_generate_agents_yaml_without_comments(self, generator, sample_agents):
        """Test generating agents YAML without comments."""
        result = generator.generate_agents_yaml(
            agents=sample_agents,
            model_override=None,
            include_comments=False
        )

        assert '# Agent Configuration' not in result

    def test_generate_agents_yaml_name_sanitization(self, generator):
        """Test that agent names are sanitized."""
        agents = [
            {
                'name': 'My Research Agent',
                'role': 'Researcher',
                'goal': 'Research',
                'backstory': 'Expert',
            }
        ]

        result = generator.generate_agents_yaml(
            agents=agents,
            model_override=None,
            include_comments=False
        )

        parsed = yaml.safe_load(result)
        assert 'my_research_agent' in parsed

    def test_generate_agents_yaml_optional_fields(self, generator):
        """Test that optional fields are included when present."""
        agents = [
            {
                'name': 'Agent',
                'role': 'Role',
                'goal': 'Goal',
                'backstory': 'Backstory',
                'max_iter': 10,
                'verbose': True,
                'allow_delegation': False,
            }
        ]

        result = generator.generate_agents_yaml(
            agents=agents,
            model_override=None,
            include_comments=False
        )

        parsed = yaml.safe_load(result)
        assert parsed['agent']['max_iter'] == 10
        assert parsed['agent']['verbose'] is True
        assert parsed['agent']['allow_delegation'] is False


class TestGenerateTasksYaml:
    """Tests for generate_tasks_yaml method."""

    @pytest.fixture
    def generator(self):
        """Create a YAMLGenerator instance."""
        return YAMLGenerator()

    @pytest.fixture
    def sample_agents(self):
        """Create sample agents."""
        return [
            {
                'id': 'agent-1',
                'name': 'Research Agent',
                'role': 'Researcher',
            },
            {
                'id': 'agent-2',
                'name': 'Writer Agent',
                'role': 'Writer',
            },
        ]

    @pytest.fixture
    def sample_tasks(self):
        """Create sample tasks."""
        return [
            {
                'id': 'task-1',
                'name': 'Research Task',
                'description': 'Research the given topic thoroughly',
                'expected_output': 'Comprehensive research report',
                'agent_id': 'agent-1',
            },
            {
                'id': 'task-2',
                'name': 'Write Task',
                'description': 'Write content based on research',
                'expected_output': 'Written article',
                'agent_id': 'agent-2',
                'context': ['task-1'],
            },
        ]

    def test_generate_tasks_yaml_basic(self, generator, sample_tasks, sample_agents):
        """Test generating basic tasks YAML."""
        result = generator.generate_tasks_yaml(
            tasks=sample_tasks,
            agents=sample_agents,
            include_comments=True
        )

        # Should be valid YAML
        parsed = yaml.safe_load(result)
        assert 'research_task' in parsed
        assert 'write_task' in parsed

    def test_generate_tasks_yaml_content(self, generator, sample_tasks, sample_agents):
        """Test generated tasks YAML content."""
        result = generator.generate_tasks_yaml(
            tasks=sample_tasks,
            agents=sample_agents,
            include_comments=False
        )

        parsed = yaml.safe_load(result)

        # Check research task
        assert parsed['research_task']['description'] == 'Research the given topic thoroughly'
        assert parsed['research_task']['expected_output'] == 'Comprehensive research report'

    def test_generate_tasks_yaml_agent_mapping(self, generator, sample_tasks, sample_agents):
        """Test that agent IDs are mapped to agent names."""
        result = generator.generate_tasks_yaml(
            tasks=sample_tasks,
            agents=sample_agents,
            include_comments=False
        )

        parsed = yaml.safe_load(result)

        # Agent should be mapped from ID to name
        assert parsed['research_task']['agent'] == 'research_agent'
        assert parsed['write_task']['agent'] == 'writer_agent'

    def test_generate_tasks_yaml_context_mapping(self, generator, sample_tasks, sample_agents):
        """Test that context task IDs are mapped to task names."""
        result = generator.generate_tasks_yaml(
            tasks=sample_tasks,
            agents=sample_agents,
            include_comments=False
        )

        parsed = yaml.safe_load(result)

        # Context should be mapped from ID to name
        assert 'context' in parsed['write_task']
        assert 'research_task' in parsed['write_task']['context']

    def test_generate_tasks_yaml_with_comments(self, generator, sample_tasks, sample_agents):
        """Test generating tasks YAML with comments."""
        result = generator.generate_tasks_yaml(
            tasks=sample_tasks,
            agents=sample_agents,
            include_comments=True
        )

        assert '# Task Configuration' in result

    def test_generate_tasks_yaml_without_comments(self, generator, sample_tasks, sample_agents):
        """Test generating tasks YAML without comments."""
        result = generator.generate_tasks_yaml(
            tasks=sample_tasks,
            agents=sample_agents,
            include_comments=False
        )

        assert '# Task Configuration' not in result

    def test_generate_tasks_yaml_default_agent_assignment(self, generator, sample_agents):
        """Test that tasks without agent_id get default agent assigned."""
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task Without Agent',
                'description': 'A task without explicit agent',
                'expected_output': 'Output',
                # No agent_id
            }
        ]

        result = generator.generate_tasks_yaml(
            tasks=tasks,
            agents=sample_agents,
            include_comments=False
        )

        parsed = yaml.safe_load(result)
        # Should assign first agent as default
        assert parsed['task_without_agent']['agent'] == 'research_agent'

    def test_generate_tasks_yaml_optional_fields(self, generator, sample_agents):
        """Test that optional fields are included when present."""
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task',
                'description': 'Description',
                'expected_output': 'Output',
                'agent_id': 'agent-1',
                'async_execution': True,
                'human_input': True,
            }
        ]

        result = generator.generate_tasks_yaml(
            tasks=tasks,
            agents=sample_agents,
            include_comments=False
        )

        parsed = yaml.safe_load(result)
        assert parsed['task']['async_execution'] is True
        assert parsed['task']['human_input'] is True

    def test_generate_tasks_yaml_name_sanitization(self, generator, sample_agents):
        """Test that task names are sanitized."""
        tasks = [
            {
                'id': 'task-1',
                'name': 'My Research Task',
                'description': 'Description',
                'expected_output': 'Output',
                'agent_id': 'agent-1',
            }
        ]

        result = generator.generate_tasks_yaml(
            tasks=tasks,
            agents=sample_agents,
            include_comments=False
        )

        parsed = yaml.safe_load(result)
        assert 'my_research_task' in parsed


class TestAgentIdMapping:
    """Tests for agent ID to name mapping logic."""

    @pytest.fixture
    def generator(self):
        """Create a YAMLGenerator instance."""
        return YAMLGenerator()

    def test_string_agent_id_mapping(self, generator):
        """Test mapping with string agent IDs."""
        agents = [{'id': 'agent-uuid', 'name': 'Test Agent'}]
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': 'agent-uuid',
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        assert parsed['task']['agent'] == 'test_agent'

    def test_integer_agent_id_mapping(self, generator):
        """Test mapping with integer agent IDs."""
        agents = [{'id': 123, 'name': 'Test Agent'}]
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': 123,
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        assert parsed['task']['agent'] == 'test_agent'

    def test_numeric_string_agent_id_mapping(self, generator):
        """Test mapping with numeric string agent IDs."""
        agents = [{'id': '456', 'name': 'Test Agent'}]
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': '456',
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        assert parsed['task']['agent'] == 'test_agent'

    def test_direct_agent_id_fallback(self, generator):
        """Test that agent_id is used directly when not found in agent map."""
        agents = [{'id': 'agent-known', 'name': 'Known Agent'}]
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': 'unknown-agent-name',
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        # Should fall back to using agent_id directly
        assert parsed['task']['agent'] == 'unknown-agent-name'

    def test_null_agent_id(self, generator):
        """Test that null agent_id results in default agent assignment."""
        agents = [{'id': 'agent-1', 'name': 'Default Agent'}]
        tasks = [
            {
                'id': 'task-1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': None,
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        # Should assign the first agent as default since agent_id is None
        assert parsed['task']['agent'] == 'default_agent'


class TestTaskConfigMerging:
    """Tests for task config dict merging into task YAML."""

    @pytest.fixture
    def generator(self):
        return YAMLGenerator()

    def test_task_config_dict_is_merged(self, generator):
        """Test that task config dict entries appear in output."""
        agents = [{'id': 'a1', 'name': 'Agent'}]
        tasks = [
            {
                'id': 't1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': 'a1',
                'config': {'temperature': 0.5, 'max_tokens': 1000},
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        assert parsed['task']['temperature'] == 0.5
        assert parsed['task']['max_tokens'] == 1000

    def test_task_config_does_not_override_existing(self, generator):
        """Test that config dict does not override existing task fields."""
        agents = [{'id': 'a1', 'name': 'Agent'}]
        tasks = [
            {
                'id': 't1',
                'name': 'Task',
                'description': 'Original description',
                'expected_output': 'Output',
                'agent_id': 'a1',
                'config': {'description': 'Overridden description'},
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        # description should not be overridden by config
        assert parsed['task']['description'] == 'Original description'

    def test_task_config_none_values_skipped(self, generator):
        """Test that None values in config dict are skipped."""
        agents = [{'id': 'a1', 'name': 'Agent'}]
        tasks = [
            {
                'id': 't1',
                'name': 'Task',
                'description': 'Desc',
                'expected_output': 'Output',
                'agent_id': 'a1',
                'config': {'extra_key': None, 'real_key': 'value'},
            }
        ]

        result = generator.generate_tasks_yaml(tasks, agents, include_comments=False)
        parsed = yaml.safe_load(result)

        assert 'extra_key' not in parsed['task']
        assert parsed['task']['real_key'] == 'value'
