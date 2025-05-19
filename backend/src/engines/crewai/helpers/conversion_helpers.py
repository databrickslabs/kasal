"""
Conversion helpers for CrewAI engine.

This module provides utility functions for converting between different formats
used by the CrewAI engine.
"""

from typing import Dict, Any, Tuple, List

def extract_crew_yaml_data(agents_yaml: Dict[str, Any], tasks_yaml: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Extract agent and task data from YAML configurations.
    
    Args:
        agents_yaml: Agent YAML configuration
        tasks_yaml: Task YAML configuration
        
    Returns:
        tuple: (agents_data, tasks_data)
    """
    # Process agents
    agents_data = []
    for agent_id, agent_config in agents_yaml.items():
        # Create a copy of the config and add the ID
        agent_data = dict(agent_config)
        agent_data["id"] = agent_id
        agents_data.append(agent_data)
    
    # Process tasks
    tasks_data = []
    for task_id, task_config in tasks_yaml.items():
        # Create a copy of the config and add the ID
        task_data = dict(task_config)
        task_data["id"] = task_id
        tasks_data.append(task_data)
    
    return agents_data, tasks_data 