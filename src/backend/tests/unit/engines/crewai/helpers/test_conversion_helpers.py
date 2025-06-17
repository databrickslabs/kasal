import pytest
from typing import Dict, Any

from src.engines.crewai.helpers.conversion_helpers import extract_crew_yaml_data


class TestConversionHelpers:
    """Test suite for conversion_helpers module."""
    
    def test_extract_crew_yaml_data_basic(self):
        """Test basic extraction of crew YAML data."""
        agents_yaml = {
            "researcher": {
                "role": "Senior Research Analyst",
                "goal": "Research AI trends",
                "backstory": "Expert in AI research"
            },
            "writer": {
                "role": "Content Writer", 
                "goal": "Write engaging content",
                "backstory": "Skilled writer"
            }
        }
        
        tasks_yaml = {
            "research_task": {
                "description": "Research latest AI developments",
                "expected_output": "Research report"
            },
            "writing_task": {
                "description": "Write blog post",
                "expected_output": "Blog post"
            }
        }
        
        agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
        
        # Verify agents data
        assert len(agents_data) == 2
        
        researcher = next(agent for agent in agents_data if agent["id"] == "researcher")
        assert researcher["role"] == "Senior Research Analyst"
        assert researcher["goal"] == "Research AI trends"
        assert researcher["backstory"] == "Expert in AI research"
        
        writer = next(agent for agent in agents_data if agent["id"] == "writer")
        assert writer["role"] == "Content Writer"
        assert writer["goal"] == "Write engaging content"
        assert writer["backstory"] == "Skilled writer"
        
        # Verify tasks data
        assert len(tasks_data) == 2
        
        research_task = next(task for task in tasks_data if task["id"] == "research_task")
        assert research_task["description"] == "Research latest AI developments"
        assert research_task["expected_output"] == "Research report"
        
        writing_task = next(task for task in tasks_data if task["id"] == "writing_task")
        assert writing_task["description"] == "Write blog post"
        assert writing_task["expected_output"] == "Blog post"
    
    def test_extract_crew_yaml_data_empty_inputs(self):
        """Test extraction with empty inputs."""
        agents_data, tasks_data = extract_crew_yaml_data({}, {})
        
        assert agents_data == []
        assert tasks_data == []
    
    def test_extract_crew_yaml_data_single_items(self):
        """Test extraction with single agent and task."""
        agents_yaml = {
            "solo_agent": {
                "role": "Solo Agent",
                "goal": "Complete tasks independently",
                "backstory": "Works alone"
            }
        }
        
        tasks_yaml = {
            "solo_task": {
                "description": "Complete solo task",
                "expected_output": "Task result"
            }
        }
        
        agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
        
        assert len(agents_data) == 1
        assert len(tasks_data) == 1
        
        assert agents_data[0]["id"] == "solo_agent"
        assert agents_data[0]["role"] == "Solo Agent"
        
        assert tasks_data[0]["id"] == "solo_task"
        assert tasks_data[0]["description"] == "Complete solo task"
    
    def test_extract_crew_yaml_data_preserves_original_config(self):
        """Test that original configuration is preserved and ID is added."""
        agents_yaml = {
            "test_agent": {
                "role": "Test Agent",
                "goal": "Test things",
                "backstory": "Testing expert",
                "custom_field": "custom_value",
                "nested_config": {
                    "param1": "value1",
                    "param2": "value2"
                }
            }
        }
        
        tasks_yaml = {
            "test_task": {
                "description": "Test task",
                "expected_output": "Test result",
                "custom_param": "custom_task_value",
                "complex_config": {
                    "setting1": True,
                    "setting2": [1, 2, 3]
                }
            }
        }
        
        agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
        
        agent = agents_data[0]
        # Verify original fields are preserved
        assert agent["role"] == "Test Agent"
        assert agent["goal"] == "Test things"
        assert agent["backstory"] == "Testing expert"
        assert agent["custom_field"] == "custom_value"
        assert agent["nested_config"]["param1"] == "value1"
        assert agent["nested_config"]["param2"] == "value2"
        # Verify ID was added
        assert agent["id"] == "test_agent"
        
        task = tasks_data[0]
        # Verify original fields are preserved
        assert task["description"] == "Test task"
        assert task["expected_output"] == "Test result"
        assert task["custom_param"] == "custom_task_value"
        assert task["complex_config"]["setting1"] is True
        assert task["complex_config"]["setting2"] == [1, 2, 3]
        # Verify ID was added
        assert task["id"] == "test_task"
    
    def test_extract_crew_yaml_data_multiple_agents_tasks(self):
        """Test extraction with multiple agents and tasks."""
        agents_yaml = {
            "agent1": {"role": "Role1"},
            "agent2": {"role": "Role2"},
            "agent3": {"role": "Role3"}
        }
        
        tasks_yaml = {
            "task1": {"description": "Task 1"},
            "task2": {"description": "Task 2"},
            "task3": {"description": "Task 3"},
            "task4": {"description": "Task 4"}
        }
        
        agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
        
        assert len(agents_data) == 3
        assert len(tasks_data) == 4
        
        # Verify all agents have IDs
        agent_ids = [agent["id"] for agent in agents_data]
        assert "agent1" in agent_ids
        assert "agent2" in agent_ids
        assert "agent3" in agent_ids
        
        # Verify all tasks have IDs
        task_ids = [task["id"] for task in tasks_data]
        assert "task1" in task_ids
        assert "task2" in task_ids
        assert "task3" in task_ids
        assert "task4" in task_ids
    
    def test_extract_crew_yaml_data_id_override(self):
        """Test that ID is added even if original config has an 'id' field."""
        agents_yaml = {
            "agent_key": {
                "id": "original_id",  # This should be overridden
                "role": "Test Agent"
            }
        }
        
        tasks_yaml = {
            "task_key": {
                "id": "original_task_id",  # This should be overridden
                "description": "Test task"
            }
        }
        
        agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
        
        # The ID should be the key from the YAML, not the original 'id' field
        assert agents_data[0]["id"] == "agent_key"
        assert tasks_data[0]["id"] == "task_key"
    
    def test_extract_crew_yaml_data_return_types(self):
        """Test that function returns the correct types."""
        agents_yaml = {"agent1": {"role": "Role1"}}
        tasks_yaml = {"task1": {"description": "Task 1"}}
        
        agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
        
        assert isinstance(agents_data, list)
        assert isinstance(tasks_data, list)
        assert isinstance(agents_data[0], dict)
        assert isinstance(tasks_data[0], dict)
    
    def test_extract_crew_yaml_data_immutability(self):
        """Test that original YAML data is not modified."""
        original_agents = {
            "agent1": {
                "role": "Test Agent",
                "goal": "Test goal"
            }
        }
        
        original_tasks = {
            "task1": {
                "description": "Test task",
                "expected_output": "Test output"
            }
        }
        
        # Make copies to compare
        agents_copy = dict(original_agents)
        tasks_copy = dict(original_tasks)
        
        agents_data, tasks_data = extract_crew_yaml_data(original_agents, original_tasks)
        
        # Verify original data was not modified
        assert original_agents == agents_copy
        assert original_tasks == tasks_copy
        
        # Verify extracted data has the ID added
        assert "id" in agents_data[0]
        assert "id" in tasks_data[0]