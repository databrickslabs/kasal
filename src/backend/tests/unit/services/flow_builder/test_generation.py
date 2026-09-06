from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.schemas.flow_generation import CrewFlowPlan, FlowGenerationRequest
from src.services.flow_builder.generation import FlowGenerationService, build_flow


def catalog():
    return {
        cid: {
            "name": f"Crew {cid}",
            "tasks": [
                {
                    "id": f"task-{cid}",
                    "name": cid,
                    "description": cid,
                    "expected_output": '{"approved": true}',
                }
            ],
        }
        for cid in "abcd"
    }


def plan(ids, links):
    return CrewFlowPlan(
        name="Review",
        explanation="Research then review",
        crew_ids=list(ids),
        links=links,
    )


def test_sequence_uses_real_tasks_and_nonoverlapping_positions():
    draft = build_flow(plan("ab", [{"source": "a", "target": "b"}]), catalog())
    assert draft.edges[0].data["listenToTaskIds"] == ["task-a"]
    assert draft.edges[0].data["targetTaskIds"] == ["task-b"]
    assert draft.nodes[0].position.x < draft.nodes[1].position.x
    assert draft.nodes[1].data.crewId == "b"


def test_parallel_branches_join_all_parents():
    links = [
        {"source": s, "target": t}
        for s, t in [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]
    ]
    draft = build_flow(plan("abcd", links), catalog())
    joins = [edge for edge in draft.edges if edge.target == "crew-d"]
    assert all(edge.data["logicType"] == "AND" for edge in joins)
    assert all(edge.data["listenToTaskIds"] == ["task-b", "task-c"] for edge in joins)
    assert draft.nodes[1].position.y != draft.nodes[2].position.y


def test_conditions_generate_state_mapping_and_otherwise():
    draft = build_flow(
        plan(
            "abc",
            [
                {
                    "source": "a",
                    "target": "b",
                    "condition": {"field": "approved", "operator": "==", "value": True},
                },
                {"source": "a", "target": "c", "otherwise": True},
            ],
        ),
        catalog(),
    )
    assert (
        draft.edges[0].data["routerCondition"]
        == "state.get('route_0_approved', '') == True"
    )
    assert draft.edges[0].data["stateMappings"][0]["sourceTaskId"] == "task-a"
    assert draft.edges[1].data["isDefaultRoute"] is True


@pytest.mark.parametrize(
    "ids,links",
    [
        ("az", [{"source": "a", "target": "z"}]),
        ("ab", [{"source": "a", "target": "b"}, {"source": "b", "target": "a"}]),
        ("ab", []),
        ("aa", []),
        (
            "ab",
            [
                {
                    "source": "a",
                    "target": "b",
                    "condition": {"field": "unknown", "operator": "==", "value": True},
                }
            ],
        ),
    ],
)
def test_invalid_plans_do_not_reach_canvas(ids, links):
    with pytest.raises(ValueError):
        build_flow(plan(ids, links), catalog())


def test_missing_capability_returns_explanation_without_partial_graph():
    draft = build_flow(
        CrewFlowPlan(
            name="Missing",
            explanation="A writer is needed",
            crew_ids=["a"],
            missing_capabilities=["Writer"],
        ),
        catalog(),
    )
    assert draft.nodes == []
    assert draft.missing_capabilities == ["Writer"]


@pytest.mark.asyncio
async def test_generation_scopes_both_crews_and_tasks_and_repairs_unknown_ids():
    service = FlowGenerationService(AsyncMock())
    service.crews.find_by_group = AsyncMock(
        return_value=[SimpleNamespace(id="a", name="Crew A", task_ids=["t"])]
    )
    service.tasks.find_by_group_ids = AsyncMock(
        return_value=[
            SimpleNamespace(
                id="t", name="Task", description="Research", expected_output="Report"
            )
        ]
    )
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.side_effect = [
            plan("z", []).model_dump_json(),
            plan("a", []).model_dump_json(),
        ]
        draft = await service.generate(
            FlowGenerationRequest(prompt="Research"),
            SimpleNamespace(group_ids=["team-1"]),
        )
    service.crews.find_by_group.assert_awaited_once_with(["team-1"])
    service.tasks.find_by_group_ids.assert_awaited_once_with(["team-1"])
    assert len(draft.nodes) == 1
    assert complete.await_count == 2


@pytest.mark.asyncio
async def test_empty_catalog_does_not_call_model():
    service = FlowGenerationService(AsyncMock())
    service.crews.find_by_group = AsyncMock(return_value=[])
    service.tasks.find_by_group_ids = AsyncMock(return_value=[])
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        draft = await service.generate(
            FlowGenerationRequest(prompt="Research"),
            SimpleNamespace(group_ids=["team-1"]),
        )
    assert draft.missing_capabilities
    complete.assert_not_called()


@pytest.mark.asyncio
async def test_generation_uses_only_active_teamspace_not_other_memberships():
    service = FlowGenerationService(AsyncMock())
    service.crews.find_by_group = AsyncMock(return_value=[])
    service.tasks.find_by_group_ids = AsyncMock(return_value=[])
    await service.generate(
        FlowGenerationRequest(prompt="Research"),
        SimpleNamespace(group_ids=["active", "other"]),
    )
    service.crews.find_by_group.assert_awaited_once_with(["active"])
    service.tasks.find_by_group_ids.assert_awaited_once_with(["active"])
