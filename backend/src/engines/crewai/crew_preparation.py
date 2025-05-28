"""
Crew preparation module for CrewAI engine.

This module handles the preparation and configuration of CrewAI agents and tasks.
"""

from typing import Dict, Any, List, Optional
import logging
from crewai import Agent, Crew, Task
from src.core.logger import LoggerManager
from src.engines.crewai.helpers.task_helpers import create_task, is_data_missing
from src.engines.crewai.helpers.agent_helpers import create_agent


logger = LoggerManager.get_instance().crew

def validate_crew_config(config: Dict[str, Any]) -> bool:
    """
    Validate crew configuration
    
    Args:
        config: Crew configuration dictionary
        
    Returns:
        True if configuration is valid
    """
    # Simple validation - check required sections
    required_sections = ['agents', 'tasks']
    for section in required_sections:
        if section not in config or not config[section]:
            logger.error(f"Missing or empty required section: {section}")
            return False
    
    return True
    
def handle_crew_error(e: Exception, message: str) -> None:
    """
    Handle crew-related errors
    
    Args:
        e: Exception that occurred
        message: Base error message
    """
    error_msg = f"{message}: {str(e)}"
    logger.error(error_msg, exc_info=True)

class CrewPreparation:
    """Handles the preparation of CrewAI agents and tasks"""
    
    def __init__(self, config: Dict[str, Any], tool_service=None, tool_factory=None):
        """
        Initialize the CrewPreparation class
        
        Args:
            config: Configuration dictionary containing crew setup
            tool_service: Tool service instance for resolving tool IDs
            tool_factory: Tool factory for creating tool instances
        """
        self.config = config
        self.agents: Dict[str, Agent] = {}
        self.tasks: List[Task] = []
        self.crew: Optional[Crew] = None
        self.tool_service = tool_service
        self.tool_factory = tool_factory
        
    async def prepare(self) -> bool:
        """
        Prepare the crew by creating agents and tasks
        
        Returns:
            bool: True if preparation was successful
        """
        try:
            # Validate configuration
            if not validate_crew_config(self.config):
                logger.error("Invalid crew configuration")
                return False
            
            # Create agents
            if not await self._create_agents():
                logger.error("Failed to create agents")
                return False
            
            # Create tasks
            if not await self._create_tasks():
                logger.error("Failed to create tasks")
                return False
            
            # Create crew
            if not await self._create_crew():
                logger.error("Failed to create crew")
                return False
            
            logger.info("Crew preparation completed successfully")
            return True
            
        except Exception as e:
            handle_crew_error(e, "Error during crew preparation")
            return False
    
    async def _create_agents(self) -> bool:
        """
        Create all agents defined in the configuration
        
        Returns:
            bool: True if all agents were created successfully
        """
        try:
            for i, agent_config in enumerate(self.config.get('agents', [])):
                # Use the agent's 'role' if 'name' is not present, or generate a name if neither exists
                agent_name = agent_config.get('name', agent_config.get('role', f'agent_{i}'))
                
                agent = await create_agent(
                    agent_key=agent_name,
                    agent_config=agent_config,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory
                )
                if not agent:
                    logger.error(f"Failed to create agent: {agent_name}")
                    return False
                    
                # Store the agent with the agent_name as key
                self.agents[agent_name] = agent
                logger.info(f"Created agent: {agent_name}")
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating agents")
            return False
    
    async def _create_tasks(self) -> bool:
        """
        Create all tasks defined in the configuration
        
        Returns:
            bool: True if all tasks were created successfully
        """
        try:
            from src.engines.crewai.helpers.task_helpers import create_task
            
            tasks = self.config.get('tasks', [])
            total_tasks = len(tasks)
            
            # Create a dictionary to store tasks by ID for reference
            task_dict = {}
            
            # First pass: create all tasks without setting context
            for i, task_config in enumerate(tasks):
                # Get the agent for this task, default to first agent if not specified
                agent_name = task_config.get('agent', 'unknown')
                agent = self.agents.get(agent_name)
                
                # Handle missing agent
                if not agent:
                    if not self.agents:
                        logger.error("No agents available for tasks")
                        return False
                    
                    # Use the first available agent as fallback
                    fallback_agent_name, agent = next(iter(self.agents.items()))
                    logger.warning(f"Invalid agent '{agent_name}' specified for task. Using '{fallback_agent_name}' instead.")
                
                # Define task_name first so it can be used in logging
                task_name = task_config.get('name', f"task_{len(self.tasks)}")
                task_id = task_config.get('id', task_name)
                
                # Store any context IDs for second pass resolution
                if "context" in task_config:
                    context_value = task_config.pop("context")
                    logger.info(f"Saved context references for task {task_name}: {context_value}")
                    # Store the references for resolution in second pass
                    if isinstance(context_value, list):
                        task_config['_context_refs'] = [str(item) for item in context_value]
                    elif isinstance(context_value, str):
                        task_config['_context_refs'] = [context_value]
                    elif isinstance(context_value, dict) and "task_ids" in context_value:
                        task_config['_context_refs'] = context_value["task_ids"]
                
                # Get the async execution setting
                is_async = task_config.get('async_execution', False)
                
                # Store original setting for later adjustment
                if is_async:
                    # Save that this task wanted to be async for logging
                    task_config['_wanted_async'] = True
                    if i < total_tasks - 1:
                        # Only the last task can be async in CrewAI, force others to be sync
                        logger.warning(f"Task '{task_name}' was set to async but isn't the last task. Only the last task can be async in CrewAI. Setting to synchronous.")
                        task_config['async_execution'] = False
                        is_async = False
                
                logger.info(f"Task '{task_name}' async_execution setting: {is_async}")
                    
                # Create the task
                task = await create_task(
                    task_key=task_name,
                    task_config=task_config,
                    agent=agent,
                    output_dir=self.config.get('output_dir'),
                    config=None,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory
                )
                
                self.tasks.append(task)
                # Store in our dictionary for context resolution
                task_dict[task_id] = task
                logger.info(f"Created task: {task_name} for agent: {agent_name}")
                
            # Second pass: Resolve context references to actual Task objects
            for task_config in tasks:
                task_id = task_config.get('id', task_config.get('name'))
                task = task_dict.get(task_id)
                
                if not task:
                    logger.warning(f"Could not find task for ID {task_id} during context resolution")
                    continue
                    
                # If this task has context references, resolve them
                if '_context_refs' in task_config:
                    context_refs = task_config['_context_refs']
                    context_tasks = []
                    
                    for ref in context_refs:
                        if ref in task_dict:
                            context_tasks.append(task_dict[ref])
                        else:
                            logger.warning(f"Could not resolve context reference '{ref}' for task {task_id}")
                    
                    if context_tasks:
                        logger.info(f"Setting context for task {task_id} to {len(context_tasks)} Task objects")
                        task.context = context_tasks
                    else:
                        logger.warning(f"No context tasks could be resolved for task {task_id}")
            
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating tasks")
            return False
    
    async def _create_crew(self) -> bool:
        """
        Create the crew with all prepared agents and tasks
        
        Returns:
            bool: True if crew was created successfully
        """
        try:
            # Get crew configuration
            crew_config = self.config.get('crew', {})
            
            # Create the crew directly instead of calling an external function
            crew_kwargs = {
                'agents': list(self.agents.values()),
                'tasks': self.tasks,
                'process': crew_config.get('process', 'sequential'),
                'verbose': True,
                'memory': True
            }
            
            # Add optional parameters if they exist in config
            if 'max_rpm' in self.config:
                crew_kwargs['max_rpm'] = self.config['max_rpm']
                
            if 'planning' in crew_config:
                crew_kwargs['planning'] = crew_config['planning']
                
            if 'planning_llm' in crew_config:
                crew_kwargs['planning_llm'] = crew_config['planning_llm']
            
            if 'reasoning' in crew_config:
                crew_kwargs['reasoning'] = crew_config['reasoning']
                
            if 'reasoning_llm' in crew_config:
                crew_kwargs['reasoning_llm'] = crew_config['reasoning_llm']
            
            # Create the crew instance
            self.crew = Crew(**crew_kwargs)
            
            if not self.crew:
                logger.error("Failed to create crew")
                return False
            
            logger.info("Created crew successfully")
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating crew")
            return False
    
    async def execute(self) -> Dict[str, Any]:
        """
        Execute the prepared crew
        
        Returns:
            Dict[str, Any]: Results from crew execution
        """
        if not self.crew:
            logger.error("Cannot execute crew: crew not prepared")
            return {"error": "Crew not prepared"}
        
        try:
            # Execute the crew
            result = await self.crew.kickoff()
            
            # Process the output
            processed_output = await process_crew_output(result)
            
            # Check if data is missing
            if is_data_missing(processed_output):
                logger.warning("Crew execution completed but data may be missing")
            
            return processed_output
            
        except Exception as e:
            handle_crew_error(e, "Error during crew execution")
            return {"error": str(e)} 