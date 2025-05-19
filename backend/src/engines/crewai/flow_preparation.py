"""
Flow preparation module for CrewAI engine.

This module handles the preparation and validation of flows before execution.
"""

import logging
from typing import Dict, Any, List, Optional
from pathlib import Path

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

class FlowPreparation:
    """
    Handles the preparation and validation of flows before execution.
    """
    
    def __init__(self, config: Dict[str, Any], output_dir: Path):
        """
        Initialize the flow preparation.
        
        Args:
            config: Normalized flow configuration
            output_dir: Directory for flow execution outputs
        """
        self.config = config
        self.output_dir = output_dir
        self.agents = {}
        self.tasks = {}
        
    def prepare(self) -> Dict[str, Any]:
        """
        Prepare the flow for execution.
        
        Returns:
            Dictionary containing prepared flow components
        """
        logger.info("Starting flow preparation")
        
        try:
            self._validate_config()
            self._prepare_agents()
            self._prepare_tasks()
            self._validate_flow()
            
            prepared_flow = {
                'agents': self.agents,
                'tasks': self.tasks,
                'flow': self.config['flow'],
                'output_dir': self.output_dir
            }
            
            logger.info("Flow preparation completed successfully")
            return prepared_flow
            
        except Exception as e:
            logger.error(f"Flow preparation failed: {str(e)}")
            raise
            
    def _validate_config(self):
        """Validate the flow configuration structure."""
        required_sections = ['agents', 'tasks', 'flow']
        for section in required_sections:
            if not self.config.get(section):
                raise ValueError(f"Missing or empty required section: {section}")
                
        # Validate flow type
        valid_flow_types = ['sequential', 'parallel', 'conditional']
        flow_type = self.config['flow'].get('type')
        if flow_type not in valid_flow_types:
            raise ValueError(f"Invalid flow type: {flow_type}. Must be one of {valid_flow_types}")
            
    def _prepare_agents(self):
        """Prepare and validate agents."""
        for agent_config in self.config['agents']:
            name = agent_config.get('name')
            if not name:
                raise ValueError("Agent must have a name")
                
            if not agent_config.get('role'):
                raise ValueError(f"Agent {name} must have a role")
                
            self.agents[name] = agent_config
            
    def _prepare_tasks(self):
        """Prepare and validate tasks."""
        for task_config in self.config['tasks']:
            name = task_config.get('name')
            if not name:
                raise ValueError("Task must have a name")
                
            if not task_config.get('description'):
                raise ValueError(f"Task {name} must have a description")
                
            agent_name = task_config.get('agent')
            if not agent_name:
                raise ValueError(f"Task {name} must be assigned to an agent")
                
            if agent_name not in self.agents:
                raise ValueError(f"Task {name} assigned to undefined agent: {agent_name}")
                
            self.tasks[name] = task_config
            
    def _validate_flow(self):
        """Validate flow configuration based on flow type."""
        flow_type = self.config['flow']['type']
        
        if flow_type == 'sequential':
            self._validate_sequential_flow()
        elif flow_type == 'parallel':
            self._validate_parallel_flow()
        elif flow_type == 'conditional':
            self._validate_conditional_flow()
            
    def _validate_sequential_flow(self):
        """Validate sequential flow configuration."""
        tasks = self.config['flow'].get('tasks', [])
        if not tasks:
            raise ValueError("Sequential flow must define tasks sequence")
            
        for task_name in tasks:
            if task_name not in self.tasks:
                raise ValueError(f"Undefined task in flow sequence: {task_name}")
                
    def _validate_parallel_flow(self):
        """Validate parallel flow configuration."""
        parallel_tasks = self.config['flow'].get('parallel_tasks', [])
        if not parallel_tasks:
            raise ValueError("Parallel flow must define parallel task groups")
            
        for task_group in parallel_tasks:
            if not isinstance(task_group, list):
                raise ValueError("Parallel task group must be a list")
                
            for task_name in task_group:
                if task_name not in self.tasks:
                    raise ValueError(f"Undefined task in parallel group: {task_name}")
                    
    def _validate_conditional_flow(self):
        """Validate conditional flow configuration."""
        conditional_tasks = self.config['flow'].get('conditional_tasks', {})
        if not conditional_tasks:
            raise ValueError("Conditional flow must define conditional tasks")
            
        for condition, task_names in conditional_tasks.items():
            if not isinstance(task_names, list):
                raise ValueError(f"Tasks for condition {condition} must be a list")
                
            for task_name in task_names:
                if task_name not in self.tasks:
                    raise ValueError(f"Undefined task in conditional flow: {task_name}") 