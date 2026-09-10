"""Apply generated MCP choices to one flow task without mutating a saved crew."""


def apply_flow_mcp_assignments(configs, task_id, crew_id, flow_data):
    matches = []
    for node in (flow_data or {}).get("nodes", []):
        data = node.get("data", {})
        if str(data.get("crewId")) != str(crew_id):
            continue
        assignment = data.get("mcpAssignments", {})
        if str(task_id) not in assignment:
            continue
        servers = assignment[str(task_id)]
        if not isinstance(servers, list) or any(
            not isinstance(s, str) or not s for s in servers
        ):
            raise ValueError("Invalid flow MCP assignment")
        matches.append(servers)
    if not matches:
        return configs
    if any(servers != matches[0] for servers in matches[1:]):
        raise ValueError("Conflicting MCP assignments for the same flow task")
    if not matches[0]:
        return configs
    existing = (configs or {}).get("MCP_SERVERS", {})
    previous = existing.get("servers", []) if isinstance(existing, dict) else existing
    return {
        **(configs or {}),
        "MCP_SERVERS": {
            **(existing if isinstance(existing, dict) else {}),
            "servers": list(dict.fromkeys([*(previous or []), *matches[0]])),
        },
    }


def flow_task_tool_ids(task_id, crew_id, flow_data):
    result = []
    for node in (flow_data or {}).get("nodes", []):
        data = node.get("data", {})
        if str(data.get("crewId")) != str(crew_id):
            continue
        ids = data.get("toolAssignments", {}).get(str(task_id), [])
        if not isinstance(ids, list) or any(
            not isinstance(tid, str) or not tid.isdecimal() for tid in ids
        ):
            raise ValueError("Invalid flow tool assignment")
        result.extend(ids)
    return list(dict.fromkeys(result))
