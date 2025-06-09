"""
Configuration Adapter for CrewAI engine.

This module provides functionality for adapting various configuration formats
to the format expected by the CrewAI engine.
"""
import logging
from typing import Dict, Any, Tuple, List, Optional

from src.schemas.execution import CrewConfig
from src.engines.crewai.helpers.conversion_helpers import extract_crew_yaml_data
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

def adapt_config(config: CrewConfig) -> Dict[str, Any]:
    """
    Adapt a CrewConfig to the engine-specific format.
    
    Args:
        config: Configuration in CrewConfig format
        
    Returns:
        Dictionary with the engine format
    """
    # Extract agents and tasks from YAML
    agents_data, tasks_data = extract_crew_yaml_data(
        config.agents_yaml, 
        config.tasks_yaml
    )
    
    # Determine tools to use based on config
    tools = []
    if config.inputs and "tools" in config.inputs:
        tools = config.inputs["tools"]
    
    # Log planning settings
    if config.planning:
        logger.info(f"Planning is enabled for execution")
    else:
        logger.debug("Planning is disabled for execution")
        
    # Log reasoning settings
    if config.reasoning:
        logger.info(f"Reasoning is enabled for execution")
    else:
        logger.debug("Reasoning is disabled for execution")
        
    # Create engine configuration
    engine_config = {
        "agents": agents_data,
        "tasks": tasks_data,
        "tools": tools,
        "crew": {
            "process": config.inputs.get("process", "sequential"),
            "verbose": True,
            "memory": True,
            "planning": config.planning,
            "reasoning": config.reasoning
        },
        "model": config.model or "gpt-4o",
        "max_rpm": config.inputs.get("max_rpm", 10),
        "output_dir": config.inputs.get("output_dir", None),
        # Include the original frontend configuration for logging
        "original_config": {
            "model": config.model,
            "llm_provider": config.llm_provider,
            "agents_yaml": config.agents_yaml,
            "tasks_yaml": config.tasks_yaml,
            "inputs": config.inputs,
            "planning": config.planning,
            "reasoning": config.reasoning
        }
    }
    
    # If planning_llm is specified in inputs, add it to the crew config
    if config.inputs and "planning_llm" in config.inputs:
        planning_llm = config.inputs["planning_llm"]
        engine_config["crew"]["planning_llm"] = planning_llm
        logger.info(f"Using specific planning LLM: {planning_llm}")
    elif config.planning:
        # Log that we're using the default model for planning
        logger.info(f"Using default model for planning: {engine_config['model']}")
    
    # If reasoning_llm is specified in inputs, add it to the crew config
    if config.inputs and "reasoning_llm" in config.inputs:
        reasoning_llm = config.inputs["reasoning_llm"]
        engine_config["crew"]["reasoning_llm"] = reasoning_llm
        logger.info(f"Using specific reasoning LLM: {reasoning_llm}")
    elif config.reasoning:
        # Log that we're using the default model for reasoning
        logger.info(f"Using default model for reasoning: {engine_config['model']}")
    
    return engine_config

def normalize_config(execution_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize execution configuration to the standard format.
    
    Args:
        execution_config: Configuration dictionary or CrewConfig object
        
    Returns:
        Normalized configuration dictionary
    """
    # Check if this is a CrewConfig object
    if isinstance(execution_config, CrewConfig):
        return adapt_config(execution_config)
    
    # Otherwise, assume it's already a dictionary in the right format
    return execution_config 

def normalize_flow_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a flow configuration to the expected format.
    
    Args:
        config: Raw flow configuration dictionary
        
    Returns:
        Normalized flow configuration dictionary
    """
    normalized = {}
    
    # Validate required sections
    required_sections = ['agents', 'tasks', 'flow']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required section '{section}' in flow configuration")
    
    # Normalize agents section
    normalized['agents'] = []
    for agent in config.get('agents', []):
        normalized_agent = {
            'name': agent.get('name'),
            'role': agent.get('role'),
            'goal': agent.get('goal'),
            'backstory': agent.get('backstory'),
            'tools': agent.get('tools', []),
            'allow_delegation': agent.get('allow_delegation', True),
            'verbose': agent.get('verbose', True),
            'memory': agent.get('memory', {}),
            'llm_config': agent.get('llm_config', {})
        }
        normalized['agents'].append(normalized_agent)
    
    # Normalize tasks section
    normalized['tasks'] = []
    for task in config.get('tasks', []):
        normalized_task = {
            'name': task.get('name'),
            'description': task.get('description'),
            'agent': task.get('agent'),
            'expected_output': task.get('expected_output'),
            'tools': task.get('tools', []),
            'context': task.get('context', {}),
            'async_execution': task.get('async_execution', False),
            'markdown': task.get('markdown', False)
        }
        normalized['tasks'].append(normalized_task)
    
    # Normalize flow section
    normalized['flow'] = {
        'type': config['flow'].get('type', 'sequential'),
        'tasks': config['flow'].get('tasks', []),
        'parallel_tasks': config['flow'].get('parallel_tasks', []),
        'conditional_tasks': config['flow'].get('conditional_tasks', {}),
        'error_handling': config['flow'].get('error_handling', {}),
        'max_iterations': config['flow'].get('max_iterations', 10),
        'timeout': config['flow'].get('timeout', 3600)
    }
    
    # Copy any additional configuration
    for key, value in config.items():
        if key not in ['agents', 'tasks', 'flow']:
            normalized[key] = value
            
    return normalized 