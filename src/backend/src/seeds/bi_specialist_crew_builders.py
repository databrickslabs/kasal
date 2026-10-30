"""Canvas-graph builders and shared config for the BI Specialist crew seeds.

``bi_specialist_crews`` (the PBI conversion flow) and
``bi_specialist_maintenance_crews`` (standalone sweeps) build each seeded crew's
ReactFlow graph (agent node, task node, agent-to-task edge) with these helpers.
Split out of ``bi_specialist_crews`` for the file-size ratchet; pure data/builders,
no I/O at import time.
"""


def _agent_node(agent_id: str, agent_data: dict, x: int, y: int) -> dict:
    """Build a ReactFlow agentNode for the crew's visual graph."""
    return {
        "id": f"agent-{agent_id}",
        "type": "agentNode",
        "position": {"x": x, "y": y},
        "data": {
            "label": agent_data["role"],
            "role": agent_data["role"],
            "goal": agent_data["goal"],
            "backstory": agent_data["backstory"],
            "tools": agent_data.get("tools", []),
            "tool_configs": agent_data.get("tool_configs"),
            "agentId": agent_id,
            "taskId": None,
            "llm": agent_data.get("llm", "databricks-claude-sonnet-4-5"),
            "function_calling_llm": None,
            "max_iter": agent_data.get("max_iter", 25),
            "max_rpm": agent_data.get("max_rpm", 10),
            "max_execution_time": agent_data.get("max_execution_time", 300),
            "verbose": agent_data.get("verbose", True),
            "allow_delegation": agent_data.get("allow_delegation", False),
            "cache": agent_data.get("cache", True),
            "memory": agent_data.get("memory", True),
            "embedder_config": agent_data.get("embedder_config"),
            "allow_code_execution": False,
            "code_execution_mode": "safe",
            "max_retry_limit": agent_data.get("max_retry_limit", 3),
            "use_system_prompt": True,
            "respect_context_window": True,
            "type": "agent",
            "inject_date": True,
        },
    }


def _task_node(task_id: str, agent_id: str, task_data: dict, x: int, y: int) -> dict:
    """Build a ReactFlow taskNode for the crew's visual graph."""
    return {
        "id": f"task-{task_id}",
        "type": "taskNode",
        "position": {"x": x, "y": y},
        "data": {
            "label": task_data["name"],
            "tools": task_data.get("tools", []),
            "tool_configs": task_data.get("tool_configs"),
            "agentId": None,
            "taskId": task_id,
            "llm": None,
            "type": "task",
            "description": task_data["description"],
            "expected_output": task_data["expected_output"],
            "config": task_data.get("config", {}),
            "async_execution": task_data.get("async_execution", False),
            "context": task_data.get("context", []),
            "human_input": task_data.get("config", {}).get("human_input", False),
            "markdown": False,
        },
    }


def _agent_to_task_edge(crew_slug: str, agent_id: str, task_id: str) -> dict:
    return {
        "id": f"edge-{crew_slug}-agent-to-task",
        "source": f"agent-{agent_id}",
        "target": f"task-{task_id}",
    }


DEFAULT_EMBEDDER = {
    "provider": "databricks",
    "config": {"model": "databricks-gte-large-en"},
}

DEFAULT_TASK_CONFIG = {
    "cache_response": False,
    "cache_ttl": 3600,
    "retry_on_fail": True,
    "max_retries": 3,
    "priority": 1,
    "error_handling": "default",
    "human_input": False,
    "markdown": False,
    "output_json": "true",
}
