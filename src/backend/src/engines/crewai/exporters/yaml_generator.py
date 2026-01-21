"""
YAML configuration generator for CrewAI agents and tasks.
"""

from typing import Dict, Any, List, Optional
import yaml
import logging

logger = logging.getLogger(__name__)


class YAMLGenerator:
    """Generate YAML configurations for agents and tasks"""

    def generate_agents_yaml(
        self,
        agents: List[Dict[str, Any]],
        model_override: Optional[str] = None,
        include_comments: bool = True
    ) -> str:
        """
        Generate agents.yaml content

        Args:
            agents: List of agent configurations
            model_override: Optional model override for all agents
            include_comments: Whether to include explanatory comments

        Returns:
            YAML string for agents configuration
        """
        agents_config = {}

        for agent in agents:
            agent_name = agent.get('name', 'agent').lower().replace(' ', '_')

            # Build agent configuration
            agent_config = {
                'role': agent.get('role', f'{agent_name} role'),
                'goal': agent.get('goal', f'{agent_name} goal'),
                'backstory': agent.get('backstory', f'{agent_name} backstory'),
            }

            # LLM configuration
            llm = model_override or agent.get('llm', 'databricks-llama-4-maverick')
            agent_config['llm'] = llm

            # Add optional configurations if present
            optional_fields = [
                'max_iter', 'max_rpm', 'max_execution_time', 'verbose',
                'allow_delegation', 'cache', 'system_template',
                'prompt_template', 'response_template'
            ]

            for field in optional_fields:
                if field in agent and agent[field] is not None:
                    agent_config[field] = agent[field]

            agents_config[agent_name] = agent_config

        # Convert to YAML
        yaml_content = yaml.dump(
            agents_config,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True
        )

        # Add header comment if requested
        if include_comments:
            header = (
                "# Agent Configuration\n"
                "#\n"
                "# Each agent is defined with:\n"
                "# - role: The function the agent performs\n"
                "# - goal: What the agent aims to achieve\n"
                "# - backstory: Context that shapes agent behavior\n"
                "# - llm: The language model to use\n"
                "#\n"
                "# You can use variables in curly braces (e.g., {topic}) that will be\n"
                "# provided at runtime through the crew's inputs.\n\n"
            )
            yaml_content = header + yaml_content

        return yaml_content

    def generate_tasks_yaml(
        self,
        tasks: List[Dict[str, Any]],
        agents: List[Dict[str, Any]],
        include_comments: bool = True
    ) -> str:
        """
        Generate tasks.yaml content

        Args:
            tasks: List of task configurations
            agents: List of agent configurations (for mapping)
            include_comments: Whether to include explanatory comments

        Returns:
            YAML string for tasks configuration
        """
        # Create agent ID to name mapping
        agent_map = {}
        for agent in agents:
            agent_id = agent.get('id')
            agent_name = agent.get('name', 'agent').lower().replace(' ', '_')
            if agent_id:
                # Store both string and int versions of ID for flexible matching
                agent_map[agent_id] = agent_name
                # Also store string version if ID is int
                if isinstance(agent_id, int):
                    agent_map[str(agent_id)] = agent_name
                # Also store int version if ID is numeric string
                elif isinstance(agent_id, str) and agent_id.isdigit():
                    agent_map[int(agent_id)] = agent_name

        logger.info(f"[YAML Debug] Agent map: {agent_map}")

        tasks_config = {}

        for task in tasks:
            task_name = task.get('name', 'task').lower().replace(' ', '_')

            # Build task configuration
            task_config = {
                'description': task.get('description', f'{task_name} description'),
                'expected_output': task.get('expected_output', f'{task_name} output'),
            }

            # Map agent ID to agent name
            agent_id = task.get('agent_id')
            logger.info(f"[YAML Debug] Task '{task_name}' has agent_id: {agent_id} (type: {type(agent_id).__name__})")

            agent_assigned = False
            if agent_id and agent_id in agent_map:
                task_config['agent'] = agent_map[agent_id]
                logger.info(f"[YAML Debug] Task '{task_name}' mapped to agent: {agent_map[agent_id]}")
                agent_assigned = True
            elif 'agent_id' in task:
                # Fallback to direct name if available
                if task['agent_id']:  # Only set if not None
                    task_config['agent'] = task['agent_id']
                    logger.info(f"[YAML Debug] Task '{task_name}' using direct agent_id: {task['agent_id']}")
                    agent_assigned = True
                else:
                    logger.warning(f"[YAML Debug] Task '{task_name}' has null agent_id")
            else:
                logger.warning(f"[YAML Debug] Task '{task_name}' has no agent_id field")

            # CRITICAL: In sequential process, all tasks must have an agent
            # Assign first agent as default if no agent is assigned
            if not agent_assigned and agents:
                first_agent_name = agents[0].get('name', 'agent').lower().replace(' ', '_')
                task_config['agent'] = first_agent_name
                logger.warning(f"[YAML Debug] Task '{task_name}' has no agent - assigning first agent: {first_agent_name}")

            # Add optional task configurations
            optional_fields = {
                'async_execution': False,
                'context': [],
                'output_file': None,
                'output_json': None,
                'output_pydantic': None,
                'callback': None,
                'human_input': False,
            }

            for field, default in optional_fields.items():
                value = task.get(field, default)
                if value != default and value is not None:
                    # Handle context - map task IDs to task names
                    if field == 'context' and isinstance(value, list):
                        context_names = []
                        for ctx_id in value:
                            # Find task name by ID
                            ctx_task = next((t for t in tasks if t.get('id') == ctx_id), None)
                            if ctx_task:
                                ctx_name = ctx_task.get('name', 'task').lower().replace(' ', '_')
                                context_names.append(ctx_name)
                        if context_names:
                            task_config['context'] = context_names
                    else:
                        task_config[field] = value

            # Add task config options if present
            if 'config' in task and isinstance(task['config'], dict):
                config = task['config']
                for key, value in config.items():
                    if value is not None and key not in task_config:
                        task_config[key] = value

            tasks_config[task_name] = task_config

        # Convert to YAML
        yaml_content = yaml.dump(
            tasks_config,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True
        )

        # Add header comment if requested
        if include_comments:
            header = (
                "# Task Configuration\n"
                "#\n"
                "# Each task is defined with:\n"
                "# - description: What needs to be done\n"
                "# - expected_output: Format and content of the result\n"
                "# - agent: Which agent handles this task\n"
                "# - context: List of task names that provide context (dependencies)\n"
                "#\n"
                "# Tasks are executed in order, with context from previous tasks\n"
                "# automatically provided to dependent tasks.\n\n"
            )
            yaml_content = header + yaml_content

        return yaml_content
