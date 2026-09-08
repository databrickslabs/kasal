"""Connection approval gates for selected conditional branches."""

from src.services.flow_builder.modules.flow_methods import FlowMethodFactory


def route_approval_gates(
    config,
    router,
    route_tasks,
    crew_name,
    upstream_method,
    sequence,
    callbacks,
    group_context,
):
    """Match both ends; another incoming branch must not impose its approval.

    A gate runs inside its selected route listener. Registering it directly on
    the upstream crew would ask for approval even when the condition is false.
    """
    nodes = {node["id"]: node.get("data", {}) for node in config.get("nodes", [])}
    target_crews = {
        str(task.get("crewId")) for task in route_tasks if task.get("crewId")
    }
    target_tasks = {str(task["id"]) for task in route_tasks if task.get("id")}
    source_crew = str(router.get("listenToCrewId") or "")
    gates = []
    for edge in config.get("edges", []):
        data = edge.get("data") or {}
        policy = data.get("hitl") or {}
        if not policy.get("enabled"):
            continue
        source, target = str(edge.get("source", "")), str(edge.get("target", ""))
        resolved_source = str(nodes.get(source, {}).get("crewId") or source)
        resolved_target = str(nodes.get(target, {}).get("crewId") or target)
        if resolved_source != source_crew:
            continue
        selected_tasks = {str(task) for task in data.get("targetTaskIds", [])}
        if selected_tasks:
            matches = bool(selected_tasks & target_tasks)
        else:
            matches = resolved_target in target_crews or target in target_tasks
        if not matches:
            continue
        edge_id = edge.get("id")
        if not edge_id:
            raise ValueError("An approval connection must have a stable edge ID")
        gates.append(
            FlowMethodFactory.create_hitl_gate_method(
                method_name=f"hitl_gate_edge_{edge_id}",
                gate_node_id=edge_id,
                gate_config={**policy, "kind": "flow_gate", "step_name": crew_name},
                previous_method_name=upstream_method,
                crew_sequence=sequence,
                callbacks=callbacks,
                group_context=group_context,
            )
        )
    return gates
