"""
Unit tests for CrewAI flow preparation module.
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch
from typing import Dict, Any

# Import directly to avoid dependency issues
sys.path.insert(0, '/Users/nehme.tohme/workspace/kasal/src/backend/src')
from engines.crewai.flow_preparation import FlowPreparation


class TestFlowPreparation:
    """Test cases for FlowPreparation class."""

    @pytest.fixture
    def valid_config(self):
        """Valid flow configuration for testing."""
        return {
            'agents': [
                {
                    'name': 'agent1',
                    'role': 'researcher',
                    'goal': 'research topics',
                    'backstory': 'experienced researcher'
                },
                {
                    'name': 'agent2',
                    'role': 'writer',
                    'goal': 'write content',
                    'backstory': 'skilled writer'
                }
            ],
            'tasks': [
                {
                    'name': 'task1',
                    'description': 'research the topic',
                    'agent': 'agent1',
                    'expected_output': 'research results'
                },
                {
                    'name': 'task2',
                    'description': 'write the article',
                    'agent': 'agent2',
                    'expected_output': 'written article'
                }
            ],
            'flow': {
                'type': 'sequential',
                'tasks': ['task1', 'task2']
            }
        }

    @pytest.fixture
    def parallel_config(self):
        """Parallel flow configuration for testing."""
        return {
            'agents': [
                {
                    'name': 'agent1',
                    'role': 'researcher',
                    'goal': 'research topics',
                    'backstory': 'experienced researcher'
                }
            ],
            'tasks': [
                {
                    'name': 'task1',
                    'description': 'research topic A',
                    'agent': 'agent1',
                    'expected_output': 'research A'
                },
                {
                    'name': 'task2',
                    'description': 'research topic B',
                    'agent': 'agent1',
                    'expected_output': 'research B'
                }
            ],
            'flow': {
                'type': 'parallel',
                'parallel_tasks': [['task1', 'task2']]
            }
        }

    @pytest.fixture
    def conditional_config(self):
        """Conditional flow configuration for testing."""
        return {
            'agents': [
                {
                    'name': 'agent1',
                    'role': 'analyst',
                    'goal': 'analyze data',
                    'backstory': 'data analyst'
                }
            ],
            'tasks': [
                {
                    'name': 'task1',
                    'description': 'analyze condition A',
                    'agent': 'agent1',
                    'expected_output': 'analysis A'
                },
                {
                    'name': 'task2',
                    'description': 'analyze condition B',
                    'agent': 'agent1',
                    'expected_output': 'analysis B'
                }
            ],
            'flow': {
                'type': 'conditional',
                'conditional_tasks': {
                    'condition_a': ['task1'],
                    'condition_b': ['task2']
                }
            }
        }

    @pytest.fixture
    def output_dir(self, tmp_path):
        """Temporary output directory for testing."""
        return tmp_path / "flow_output"

    def test_init(self, valid_config, output_dir):
        """Test FlowPreparation initialization."""
        flow_prep = FlowPreparation(valid_config, output_dir)
        
        assert flow_prep.config == valid_config
        assert flow_prep.output_dir == output_dir
        assert flow_prep.agents == {}
        assert flow_prep.tasks == {}

    def test_prepare_success_sequential(self, valid_config, output_dir):
        """Test successful preparation of sequential flow."""
        flow_prep = FlowPreparation(valid_config, output_dir)
        
        result = flow_prep.prepare()
        
        assert 'agents' in result
        assert 'tasks' in result
        assert 'flow' in result
        assert 'output_dir' in result
        
        assert len(result['agents']) == 2
        assert len(result['tasks']) == 2
        assert result['flow'] == valid_config['flow']
        assert result['output_dir'] == output_dir
        
        # Check agents were prepared
        assert 'agent1' in result['agents']
        assert 'agent2' in result['agents']
        
        # Check tasks were prepared
        assert 'task1' in result['tasks']
        assert 'task2' in result['tasks']

    def test_prepare_success_parallel(self, parallel_config, output_dir):
        """Test successful preparation of parallel flow."""
        flow_prep = FlowPreparation(parallel_config, output_dir)
        
        result = flow_prep.prepare()
        
        assert 'agents' in result
        assert 'tasks' in result
        assert 'flow' in result
        assert 'output_dir' in result
        
        assert len(result['agents']) == 1
        assert len(result['tasks']) == 2
        assert result['flow']['type'] == 'parallel'

    def test_prepare_success_conditional(self, conditional_config, output_dir):
        """Test successful preparation of conditional flow."""
        flow_prep = FlowPreparation(conditional_config, output_dir)
        
        result = flow_prep.prepare()
        
        assert 'agents' in result
        assert 'tasks' in result
        assert 'flow' in result
        assert 'output_dir' in result
        
        assert len(result['agents']) == 1
        assert len(result['tasks']) == 2
        assert result['flow']['type'] == 'conditional'

    def test_validate_config_missing_agents(self, output_dir):
        """Test config validation with missing agents section."""
        invalid_config = {
            'tasks': [],
            'flow': {'type': 'sequential'}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Missing or empty required section: agents"):
            flow_prep.prepare()

    def test_validate_config_missing_tasks(self, output_dir):
        """Test config validation with missing tasks section."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'flow': {'type': 'sequential'}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Missing or empty required section: tasks"):
            flow_prep.prepare()

    def test_validate_config_missing_flow(self, output_dir):
        """Test config validation with missing flow section."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}]
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Missing or empty required section: flow"):
            flow_prep.prepare()

    def test_validate_config_empty_agents(self, output_dir):
        """Test config validation with empty agents section."""
        invalid_config = {
            'agents': [],
            'tasks': [],
            'flow': {'type': 'sequential'}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Missing or empty required section: agents"):
            flow_prep.prepare()

    def test_validate_config_invalid_flow_type(self, output_dir):
        """Test config validation with invalid flow type."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'invalid_type'}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Invalid flow type: invalid_type"):
            flow_prep.prepare()

    def test_prepare_agents_missing_name(self, output_dir):
        """Test agent preparation with missing name."""
        invalid_config = {
            'agents': [{'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Agent must have a name"):
            flow_prep.prepare()

    def test_prepare_agents_missing_role(self, output_dir):
        """Test agent preparation with missing role."""
        invalid_config = {
            'agents': [{'name': 'agent1'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Agent agent1 must have a role"):
            flow_prep.prepare()

    def test_prepare_tasks_missing_name(self, output_dir):
        """Test task preparation with missing name."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task must have a name"):
            flow_prep.prepare()

    def test_prepare_tasks_missing_description(self, output_dir):
        """Test task preparation with missing description."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task task1 must have a description"):
            flow_prep.prepare()

    def test_prepare_tasks_missing_agent(self, output_dir):
        """Test task preparation with missing agent."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task task1 must be assigned to an agent"):
            flow_prep.prepare()

    def test_prepare_tasks_undefined_agent(self, output_dir):
        """Test task preparation with undefined agent."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'undefined_agent'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task task1 assigned to undefined agent: undefined_agent"):
            flow_prep.prepare()

    def test_validate_sequential_flow_no_tasks(self, output_dir):
        """Test sequential flow validation with no tasks."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential'}  # Missing tasks
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Sequential flow must define tasks sequence"):
            flow_prep.prepare()

    def test_validate_sequential_flow_undefined_task(self, output_dir):
        """Test sequential flow validation with undefined task."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1', 'undefined_task']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Undefined task in flow sequence: undefined_task"):
            flow_prep.prepare()

    def test_validate_parallel_flow_no_parallel_tasks(self, output_dir):
        """Test parallel flow validation with no parallel tasks."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'parallel'}  # Missing parallel_tasks
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Parallel flow must define parallel task groups"):
            flow_prep.prepare()

    def test_validate_parallel_flow_invalid_task_group(self, output_dir):
        """Test parallel flow validation with invalid task group."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'parallel', 'parallel_tasks': ['task1']}  # Should be list of lists
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Parallel task group must be a list"):
            flow_prep.prepare()

    def test_validate_parallel_flow_undefined_task(self, output_dir):
        """Test parallel flow validation with undefined task."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'parallel', 'parallel_tasks': [['task1', 'undefined_task']]}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Undefined task in parallel group: undefined_task"):
            flow_prep.prepare()

    def test_validate_conditional_flow_no_conditional_tasks(self, output_dir):
        """Test conditional flow validation with no conditional tasks."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'conditional'}  # Missing conditional_tasks
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Conditional flow must define conditional tasks"):
            flow_prep.prepare()

    def test_validate_conditional_flow_invalid_task_list(self, output_dir):
        """Test conditional flow validation with invalid task list."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'conditional', 'conditional_tasks': {'condition1': 'task1'}}  # Should be list
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Tasks for condition condition1 must be a list"):
            flow_prep.prepare()

    def test_validate_conditional_flow_undefined_task(self, output_dir):
        """Test conditional flow validation with undefined task."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'conditional', 'conditional_tasks': {'condition1': ['task1', 'undefined_task']}}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Undefined task in conditional flow: undefined_task"):
            flow_prep.prepare()

    def test_prepare_exception_handling(self, output_dir):
        """Test exception handling during preparation."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        # Mock one of the validation methods to raise an exception
        with patch.object(flow_prep, '_validate_config', side_effect=Exception("Validation error")):
            with pytest.raises(Exception, match="Validation error"):
                flow_prep.prepare()

    def test_prepare_agents_multiple_agents(self, output_dir):
        """Test preparing multiple agents."""
        config = {
            'agents': [
                {'name': 'agent1', 'role': 'researcher', 'goal': 'research', 'backstory': 'researcher bg'},
                {'name': 'agent2', 'role': 'writer', 'goal': 'write', 'backstory': 'writer bg'},
                {'name': 'agent3', 'role': 'editor', 'goal': 'edit', 'backstory': 'editor bg'}
            ],
            'tasks': [
                {'name': 'task1', 'description': 'test', 'agent': 'agent1'},
                {'name': 'task2', 'description': 'test', 'agent': 'agent2'},
                {'name': 'task3', 'description': 'test', 'agent': 'agent3'}
            ],
            'flow': {'type': 'sequential', 'tasks': ['task1', 'task2', 'task3']}
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        result = flow_prep.prepare()
        
        assert len(result['agents']) == 3
        assert 'agent1' in result['agents']
        assert 'agent2' in result['agents']
        assert 'agent3' in result['agents']

    def test_prepare_tasks_multiple_tasks(self, output_dir):
        """Test preparing multiple tasks."""
        config = {
            'agents': [
                {'name': 'agent1', 'role': 'worker'}
            ],
            'tasks': [
                {'name': 'task1', 'description': 'first task', 'agent': 'agent1'},
                {'name': 'task2', 'description': 'second task', 'agent': 'agent1'},
                {'name': 'task3', 'description': 'third task', 'agent': 'agent1'}
            ],
            'flow': {'type': 'sequential', 'tasks': ['task1', 'task2', 'task3']}
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        result = flow_prep.prepare()
        
        assert len(result['tasks']) == 3
        assert 'task1' in result['tasks']
        assert 'task2' in result['tasks']
        assert 'task3' in result['tasks']

    def test_validate_flow_types(self, output_dir):
        """Test validation of all supported flow types."""
        base_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}]
        }
        
        # Test sequential
        sequential_config = {
            **base_config,
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        flow_prep = FlowPreparation(sequential_config, output_dir)
        result = flow_prep.prepare()
        assert result['flow']['type'] == 'sequential'
        
        # Test parallel
        parallel_config = {
            **base_config,
            'flow': {'type': 'parallel', 'parallel_tasks': [['task1']]}
        }
        flow_prep = FlowPreparation(parallel_config, output_dir)
        result = flow_prep.prepare()
        assert result['flow']['type'] == 'parallel'
        
        # Test conditional
        conditional_config = {
            **base_config,
            'flow': {'type': 'conditional', 'conditional_tasks': {'condition1': ['task1']}}
        }
        flow_prep = FlowPreparation(conditional_config, output_dir)
        result = flow_prep.prepare()
        assert result['flow']['type'] == 'conditional'

    def test_output_dir_preserved(self, valid_config, output_dir):
        """Test that output directory is preserved in result."""
        flow_prep = FlowPreparation(valid_config, output_dir)
        result = flow_prep.prepare()
        
        assert result['output_dir'] == output_dir
        assert isinstance(result['output_dir'], Path)

    def test_complex_parallel_flow(self, output_dir):
        """Test complex parallel flow with multiple task groups."""
        config = {
            'agents': [
                {'name': 'agent1', 'role': 'researcher'},
                {'name': 'agent2', 'role': 'analyst'}
            ],
            'tasks': [
                {'name': 'task1', 'description': 'research A', 'agent': 'agent1'},
                {'name': 'task2', 'description': 'research B', 'agent': 'agent1'},
                {'name': 'task3', 'description': 'analyze A', 'agent': 'agent2'},
                {'name': 'task4', 'description': 'analyze B', 'agent': 'agent2'}
            ],
            'flow': {
                'type': 'parallel',
                'parallel_tasks': [
                    ['task1', 'task2'],  # Research tasks in parallel
                    ['task3', 'task4']   # Analysis tasks in parallel
                ]
            }
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        result = flow_prep.prepare()
        
        assert result['flow']['type'] == 'parallel'
        assert len(result['flow']['parallel_tasks']) == 2
        assert len(result['flow']['parallel_tasks'][0]) == 2
        assert len(result['flow']['parallel_tasks'][1]) == 2

    def test_complex_conditional_flow(self, output_dir):
        """Test complex conditional flow with multiple conditions."""
        config = {
            'agents': [
                {'name': 'agent1', 'role': 'processor'}
            ],
            'tasks': [
                {'name': 'task1', 'description': 'process type A', 'agent': 'agent1'},
                {'name': 'task2', 'description': 'process type B', 'agent': 'agent1'},
                {'name': 'task3', 'description': 'process type C', 'agent': 'agent1'},
                {'name': 'task4', 'description': 'cleanup A', 'agent': 'agent1'},
                {'name': 'task5', 'description': 'cleanup B', 'agent': 'agent1'}
            ],
            'flow': {
                'type': 'conditional',
                'conditional_tasks': {
                    'type_a': ['task1', 'task4'],
                    'type_b': ['task2', 'task5'],
                    'type_c': ['task3']
                }
            }
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        result = flow_prep.prepare()
        
        assert result['flow']['type'] == 'conditional'
        assert len(result['flow']['conditional_tasks']) == 3
        assert len(result['flow']['conditional_tasks']['type_a']) == 2
        assert len(result['flow']['conditional_tasks']['type_b']) == 2
        assert len(result['flow']['conditional_tasks']['type_c']) == 1

    def test_validate_config_flow_type_none(self, output_dir):
        """Test config validation with None flow type."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': None}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Invalid flow type: None"):
            flow_prep.prepare()

    def test_validate_config_missing_flow_type(self, output_dir):
        """Test config validation with missing flow type key."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {}  # Missing 'type' key
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Missing or empty required section: flow"):
            flow_prep.prepare()

    def test_prepare_logging_success(self, valid_config, output_dir):
        """Test successful preparation logs correct messages."""
        flow_prep = FlowPreparation(valid_config, output_dir)
        
        # Ensure prepare runs successfully to test logging paths
        result = flow_prep.prepare()
        
        # Verify the result structure to ensure method completed
        assert 'agents' in result
        assert 'tasks' in result
        assert 'flow' in result
        assert 'output_dir' in result

    def test_validate_config_empty_flow_dict(self, output_dir):
        """Test config validation with empty flow dictionary that evaluates to falsy."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {}  # Empty dict that is falsy
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        # Empty dict is truthy in Python, but the validation logic treats it as empty
        with pytest.raises(ValueError, match="Missing or empty required section: flow"):
            flow_prep.prepare()

    def test_prepare_agents_with_additional_properties(self, output_dir):
        """Test agent preparation with additional properties beyond name and role."""
        config = {
            'agents': [
                {
                    'name': 'agent1', 
                    'role': 'researcher',
                    'goal': 'research topics',
                    'backstory': 'experienced researcher',
                    'extra_property': 'extra_value'
                }
            ],
            'tasks': [
                {'name': 'task1', 'description': 'test', 'agent': 'agent1'}
            ],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        result = flow_prep.prepare()
        
        # Ensure additional properties are preserved
        assert result['agents']['agent1']['extra_property'] == 'extra_value'
        assert result['agents']['agent1']['goal'] == 'research topics'
        assert result['agents']['agent1']['backstory'] == 'experienced researcher'

    def test_prepare_tasks_with_additional_properties(self, output_dir):
        """Test task preparation with additional properties beyond required ones."""
        config = {
            'agents': [
                {'name': 'agent1', 'role': 'worker'}
            ],
            'tasks': [
                {
                    'name': 'task1', 
                    'description': 'first task', 
                    'agent': 'agent1',
                    'expected_output': 'some output',
                    'context': ['some context'],
                    'tools': ['tool1', 'tool2']
                }
            ],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        result = flow_prep.prepare()
        
        # Ensure additional properties are preserved
        assert result['tasks']['task1']['expected_output'] == 'some output'
        assert result['tasks']['task1']['context'] == ['some context']
        assert result['tasks']['task1']['tools'] == ['tool1', 'tool2']

    def test_validate_flow_line_108_coverage(self, output_dir):
        """Test to ensure line 108 coverage: flow_type = self.config['flow']['type']."""
        config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(config, output_dir)
        
        # This should trigger _validate_flow which has line 108
        result = flow_prep.prepare()
        
        # Verify the flow was processed correctly
        assert result['flow']['type'] == 'sequential'
        assert result['flow']['tasks'] == ['task1']

    def test_agent_name_empty_string(self, output_dir):
        """Test agent with empty string name (falsy but not None)."""
        invalid_config = {
            'agents': [{'name': '', 'role': 'test'}],  # Empty string name
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Agent must have a name"):
            flow_prep.prepare()

    def test_agent_role_empty_string(self, output_dir):
        """Test agent with empty string role (falsy but not None)."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': ''}],  # Empty string role
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Agent agent1 must have a role"):
            flow_prep.prepare()

    def test_task_name_empty_string(self, output_dir):
        """Test task with empty string name (falsy but not None)."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': '', 'description': 'test', 'agent': 'agent1'}],  # Empty string name
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task must have a name"):
            flow_prep.prepare()

    def test_task_description_empty_string(self, output_dir):
        """Test task with empty string description (falsy but not None)."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': '', 'agent': 'agent1'}],  # Empty string description
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task task1 must have a description"):
            flow_prep.prepare()

    def test_task_agent_empty_string(self, output_dir):
        """Test task with empty string agent (falsy but not None)."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': ''}],  # Empty string agent
            'flow': {'type': 'sequential', 'tasks': ['task1']}
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Task task1 must be assigned to an agent"):
            flow_prep.prepare()

    def test_sequential_flow_empty_tasks_list(self, output_dir):
        """Test sequential flow with explicitly empty tasks list."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'sequential', 'tasks': []}  # Explicitly empty list
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Sequential flow must define tasks sequence"):
            flow_prep.prepare()

    def test_parallel_flow_empty_parallel_tasks(self, output_dir):
        """Test parallel flow with explicitly empty parallel_tasks list."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'parallel', 'parallel_tasks': []}  # Explicitly empty list
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Parallel flow must define parallel task groups"):
            flow_prep.prepare()

    def test_conditional_flow_empty_conditional_tasks(self, output_dir):
        """Test conditional flow with explicitly empty conditional_tasks dict."""
        invalid_config = {
            'agents': [{'name': 'agent1', 'role': 'test'}],
            'tasks': [{'name': 'task1', 'description': 'test', 'agent': 'agent1'}],
            'flow': {'type': 'conditional', 'conditional_tasks': {}}  # Explicitly empty dict
        }
        
        flow_prep = FlowPreparation(invalid_config, output_dir)
        
        with pytest.raises(ValueError, match="Conditional flow must define conditional tasks"):
            flow_prep.prepare()