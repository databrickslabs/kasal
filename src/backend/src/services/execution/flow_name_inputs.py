"""Derive naming inputs from flow nodes without running the flow."""

import logging
import traceback
from src.schemas.execution import CrewConfig

logger = logging.getLogger("src.services.execution.service")


def extract_flow_name_inputs(config: CrewConfig) -> tuple:
    """
    Extract agents and tasks information from flow configuration for name generation.

    For flow executions, agents and tasks are stored in nodes (flow_config.startingPoints,
    flow_config.listeners) rather than in agents_yaml/tasks_yaml. This method extracts
    that information and formats it for the execution name generation service.

    Args:
        config: The CrewConfig containing flow configuration

    Returns:
        Tuple of (agents_yaml, tasks_yaml) dictionaries for name generation
    """
    agents_yaml = {}
    tasks_yaml = {}

    try:
        # Get nodes and flow_config from the config
        nodes = config.nodes if hasattr(config, "nodes") and config.nodes else []
        flow_config = (
            config.flow_config
            if hasattr(config, "flow_config") and config.flow_config
            else {}
        )

        logger.info(
            f"[_extract_agents_tasks_from_flow_config] Processing {len(nodes)} nodes"
        )

        # Extract from nodes (direct node data)
        for node in nodes:
            node_type = node.get("type", "").lower()
            node_data = node.get("data", {})
            node_id = node.get("id", "")

            if node_type == "crewnode":
                # Extract crew information which contains agents and tasks
                crew_name = node_data.get("label", node_data.get("name", "Crew"))
                all_agents = node_data.get("allAgents", node_data.get("agents", []))
                all_tasks = node_data.get("allTasks", node_data.get("tasks", []))

                logger.info(
                    f"[_extract_agents_tasks_from_flow_config] Found crewNode with {len(all_agents)} agents and {len(all_tasks)} tasks"
                )

                # Extract agents
                for agent in all_agents:
                    agent_id = agent.get("id", f"agent_{len(agents_yaml)}")
                    agents_yaml[agent_id] = {
                        "role": agent.get("role", agent.get("name", "Agent")),
                        "goal": agent.get("goal", ""),
                        "backstory": agent.get("backstory", ""),
                    }

                # Extract tasks
                for task in all_tasks:
                    task_id = task.get("id", f"task_{len(tasks_yaml)}")
                    tasks_yaml[task_id] = {
                        "name": task.get(
                            "name", task.get("description", "Task")[:50]
                        ),
                        "description": task.get("description", ""),
                        "expected_output": task.get(
                            "expected_output", task.get("expectedOutput", "")
                        ),
                    }

            elif node_type == "agentnode":
                # Extract single agent
                agent_id = node_data.get("agentId", node_id)
                agents_yaml[agent_id] = {
                    "role": node_data.get("role", node_data.get("label", "Agent")),
                    "goal": node_data.get("goal", ""),
                    "backstory": node_data.get("backstory", ""),
                }

            elif node_type == "tasknode":
                # Extract single task
                task_id = node_data.get("taskId", node_id)
                tasks_yaml[task_id] = {
                    "name": node_data.get("name", node_data.get("label", "Task")),
                    "description": node_data.get("description", ""),
                    "expected_output": node_data.get(
                        "expected_output", node_data.get("expectedOutput", "")
                    ),
                }

        # Also extract from flow_config's startingPoints and listeners
        starting_points = flow_config.get("startingPoints", [])
        listeners = flow_config.get("listeners", [])

        logger.info(
            f"[_extract_agents_tasks_from_flow_config] Processing {len(starting_points)} starting points and {len(listeners)} listeners"
        )

        # Process starting points
        for sp in starting_points:
            node_type = sp.get("nodeType", "")
            node_data = sp.get("nodeData", {})

            if node_type == "crewNode":
                # Extract from crew node
                all_agents = node_data.get("allAgents", node_data.get("agents", []))
                all_tasks = node_data.get("allTasks", node_data.get("tasks", []))

                for agent in all_agents:
                    agent_id = agent.get("id", f"agent_sp_{len(agents_yaml)}")
                    if agent_id not in agents_yaml:
                        agents_yaml[agent_id] = {
                            "role": agent.get("role", agent.get("name", "Agent")),
                            "goal": agent.get("goal", ""),
                            "backstory": agent.get("backstory", ""),
                        }

                for task in all_tasks:
                    task_id = task.get("id", f"task_sp_{len(tasks_yaml)}")
                    if task_id not in tasks_yaml:
                        tasks_yaml[task_id] = {
                            "name": task.get(
                                "name",
                                (
                                    task.get("description", "Task")[:50]
                                    if task.get("description")
                                    else "Task"
                                ),
                            ),
                            "description": task.get("description", ""),
                            "expected_output": task.get(
                                "expected_output", task.get("expectedOutput", "")
                            ),
                        }

            # Also extract crew info if present at top level of starting point
            crew_name = sp.get("crewName", "")
            if crew_name and crew_name not in agents_yaml:
                agents_yaml[f"crew_{crew_name}"] = {
                    "role": crew_name,
                    "goal": f"Execute {crew_name} workflow",
                    "backstory": "",
                }

        # Process listeners (same structure as starting points)
        for listener in listeners:
            node_type = listener.get("nodeType", "")
            node_data = listener.get("nodeData", {})

            if node_type == "crewNode":
                all_agents = node_data.get("allAgents", node_data.get("agents", []))
                all_tasks = node_data.get("allTasks", node_data.get("tasks", []))

                for agent in all_agents:
                    agent_id = agent.get("id", f"agent_listener_{len(agents_yaml)}")
                    if agent_id not in agents_yaml:
                        agents_yaml[agent_id] = {
                            "role": agent.get("role", agent.get("name", "Agent")),
                            "goal": agent.get("goal", ""),
                            "backstory": agent.get("backstory", ""),
                        }

                for task in all_tasks:
                    task_id = task.get("id", f"task_listener_{len(tasks_yaml)}")
                    if task_id not in tasks_yaml:
                        tasks_yaml[task_id] = {
                            "name": task.get(
                                "name",
                                (
                                    task.get("description", "Task")[:50]
                                    if task.get("description")
                                    else "Task"
                                ),
                            ),
                            "description": task.get("description", ""),
                            "expected_output": task.get(
                                "expected_output", task.get("expectedOutput", "")
                            ),
                        }

            # Also extract crew info if present at top level
            crew_name = listener.get("crewName", "")
            if crew_name and f"crew_{crew_name}" not in agents_yaml:
                agents_yaml[f"crew_{crew_name}"] = {
                    "role": crew_name,
                    "goal": f"Execute {crew_name} workflow",
                    "backstory": "",
                }

        logger.info(
            f"[_extract_agents_tasks_from_flow_config] Final extraction: {len(agents_yaml)} agents, {len(tasks_yaml)} tasks"
        )

    except Exception as e:
        logger.error(
            f"[_extract_agents_tasks_from_flow_config] Error extracting agents/tasks from flow config: {str(e)}"
        )
        logger.error(traceback.format_exc())
        # Return empty dicts on error - the fallback name generation will handle it

    return agents_yaml, tasks_yaml
