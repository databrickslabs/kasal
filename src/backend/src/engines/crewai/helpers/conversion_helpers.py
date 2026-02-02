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
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("[extract_crew_yaml_data] Starting extraction of crew YAML data")
    logger.info(f"[extract_crew_yaml_data] Number of agents: {len(agents_yaml)}")
    logger.info(f"[extract_crew_yaml_data] Number of tasks: {len(tasks_yaml)}")
    
    # Process agents
    agents_data = []
    for agent_id, agent_config in agents_yaml.items():
        # Create a copy of the config and add the ID
        agent_data = dict(agent_config)
        agent_data["id"] = agent_id
        
        # Log agent details including knowledge_sources
        logger.info(f"[extract_crew_yaml_data] Processing agent {agent_id}")
        logger.info(f"[extract_crew_yaml_data] Agent {agent_id} keys: {list(agent_config.keys())}")
        
        # Check specifically for knowledge_sources
        if "knowledge_sources" in agent_config:
            knowledge_sources = agent_config["knowledge_sources"]
            logger.info(f"[extract_crew_yaml_data] Agent {agent_id} has {len(knowledge_sources)} knowledge_sources")
            for idx, source in enumerate(knowledge_sources):
                logger.info(f"[extract_crew_yaml_data] Agent {agent_id} knowledge_source[{idx}]: {source}")
        else:
            logger.warning(f"[extract_crew_yaml_data] Agent {agent_id} has NO knowledge_sources field")
        
        agents_data.append(agent_data)
    
    # Process tasks
    tasks_data = []
    for task_id, task_config in tasks_yaml.items():
        # Create a copy of the config and add the ID
        task_data = dict(task_config)
        task_data["id"] = task_id

        # Log task details including tool_configs
        logger.info(f"[extract_crew_yaml_data] Processing task {task_id}")
        logger.info(f"[extract_crew_yaml_data] Task {task_id} keys: {list(task_config.keys())}")

        # Check for tool_configs
        if "tool_configs" in task_config:
            tool_configs = task_config["tool_configs"]
            logger.info(f"[extract_crew_yaml_data] Task {task_id} has tool_configs with keys: {list(tool_configs.keys())}")
            # Log preview of each tool config (mask secrets)
            for tool_name, tool_cfg in tool_configs.items():
                safe_cfg = {k: (v[:30] + '...' if isinstance(v, str) and len(v) > 30 else v)
                           for k, v in (tool_cfg if isinstance(tool_cfg, dict) else {}).items()
                           if 'secret' not in k.lower() and v}
                logger.info(f"[extract_crew_yaml_data] Task {task_id} tool_config[{tool_name}]: {safe_cfg}")
        else:
            logger.info(f"[extract_crew_yaml_data] Task {task_id} has NO tool_configs field")

        tasks_data.append(task_data)
    
    logger.info(f"[extract_crew_yaml_data] Extraction complete: {len(agents_data)} agents, {len(tasks_data)} tasks")
    return agents_data, tasks_data 