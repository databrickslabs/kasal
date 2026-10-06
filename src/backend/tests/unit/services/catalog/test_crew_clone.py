"""Pure clone helpers: rebuild the canvas graph with the clone's new IDs, and
reconstruct Create schemas from stored models."""

from types import SimpleNamespace

from src.services.catalog.crew_clone import (
    agent_create_from_model,
    remap_context,
    remap_crew_graph,
    task_create_from_model,
)


def test_remap_crew_graph_rewrites_ids_data_and_edges():
    nodes = [
        {"id": "agent-A", "type": "agentNode", "data": {"agentId": "A", "label": "x"}},
        {
            "id": "task-T1",
            "type": "taskNode",
            "data": {"taskId": "T1", "context": ["T2"], "label": "t1"},
        },
    ]
    edges = [
        {
            "id": "reactflow__edge-agent-A-task-T1",
            "source": "agent-A",
            "target": "task-T1",
        }
    ]
    new_nodes, new_edges = remap_crew_graph(
        nodes, edges, {"A": "A2"}, {"T1": "T1b", "T2": "T2b"}
    )

    assert new_nodes[0]["id"] == "agent-A2"
    assert new_nodes[0]["data"]["agentId"] == "A2"
    assert new_nodes[1]["id"] == "task-T1b"
    assert new_nodes[1]["data"]["taskId"] == "T1b"
    assert new_nodes[1]["data"]["context"] == ["T2b"]  # dependency repointed
    assert new_edges[0]["source"] == "agent-A2"
    assert new_edges[0]["target"] == "task-T1b"
    assert new_edges[0]["id"] == "reactflow__edge-agent-A2-task-T1b"


def test_remap_crew_graph_does_not_mutate_source():
    nodes = [{"id": "task-T1", "data": {"taskId": "T1"}}]
    remap_crew_graph(nodes, [], {}, {"T1": "T1b"})
    assert nodes[0]["id"] == "task-T1"  # original untouched (deep-copied)


def test_remap_crew_graph_leaves_unmapped_ids():
    nodes = [{"id": "task-UNKNOWN", "data": {"taskId": "UNKNOWN"}}]
    out, _ = remap_crew_graph(nodes, [], {}, {"T1": "T1b"})
    assert out[0]["id"] == "task-UNKNOWN"
    assert out[0]["data"]["taskId"] == "UNKNOWN"


def test_agent_create_from_model_copies_fields_and_drops_none():
    agent = SimpleNamespace(
        name="Analyst",
        role="r",
        goal="g",
        backstory="b",
        llm="databricks-x",
        tool_configs=None,  # dropped → schema default applies
        max_iter=25,
    )
    ac = agent_create_from_model(agent)
    assert ac.name == "Analyst" and ac.role == "r"
    assert ac.llm == "databricks-x"


def test_task_create_from_model_remaps_agent_and_clears_context():
    task = SimpleNamespace(
        name="T",
        description="d",
        expected_output="e",
        agent_id="A",
        context=["T2"],
        tools=[],
    )
    tc = task_create_from_model(task, {"A": "A2"})
    assert tc.agent_id == "A2"  # repointed at the cloned agent
    assert tc.context == []  # deferred to the caller's second pass


def test_task_create_from_model_keeps_unmapped_agent():
    task = SimpleNamespace(
        name="T", description="d", expected_output="e", agent_id="Z", tools=[]
    )
    tc = task_create_from_model(task, {"A": "A2"})
    assert tc.agent_id == "Z"


def test_remap_context_maps_known_and_keeps_unknown():
    assert remap_context(["T1", "X"], {"T1": "T1b"}) == ["T1b", "X"]
    assert remap_context(None, {"T1": "T1b"}) == []
