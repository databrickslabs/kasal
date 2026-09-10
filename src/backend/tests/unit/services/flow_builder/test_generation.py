import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.schemas.flow_generation import CrewFlowPlan, FlowGenerationRequest
from src.services.flow_builder.generation import FlowGenerationService, build_flow
from src.services.flow_builder.generation_context import FlowPlanningStep
from src.services.flow_builder.generation_intent import (
    FlowIntent,
    analyze_flow_intent,
    validate_stage_assignments,
)


@pytest.fixture(autouse=True)
def requested_intent():
    # Selection tests keep request interpretation fixed; interpretation itself
    # is tested below without this service-level patch.
    with patch(
        "src.services.flow_builder.generation.analyze_flow_intent",
        new_callable=AsyncMock,
    ) as analyze:
        analyze.return_value = FlowIntent(stages=["Research", "Presentation"])
        yield analyze


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
    return FlowPlanningStep(
        name="Review",
        explanation="Research then review",
        crew_ids=list(ids),
        links=links,
        stage_assignments={f"stage_{index + 1}": cid for index, cid in enumerate(ids)},
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


def test_independent_requests_create_two_roots_without_an_invented_join():
    draft = build_flow(plan("ab", []), catalog())
    assert [node.data.crewId for node in draft.nodes] == ["a", "b"]
    assert draft.edges == []
    assert draft.nodes[0].position.x == draft.nodes[1].position.x
    assert abs(draft.nodes[0].position.y - draft.nodes[1].position.y) >= 200


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
        ("abc", [{"source": "b", "target": "c"}, {"source": "c", "target": "b"}]),
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
async def test_generation_scopes_both_crews_and_tasks_and_repairs_unknown_ids(
    requested_intent,
):
    requested_intent.return_value = FlowIntent(stages=["Research"])
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


def generation_service():
    service = FlowGenerationService(AsyncMock())
    service.crews.find_by_group = AsyncMock(
        return_value=[
            SimpleNamespace(id=cid, name=f"Crew {cid}", task_ids=[f"task-{cid}"])
            for cid in "abc"
        ]
    )
    service.tasks.find_by_group_ids = AsyncMock(
        return_value=[
            SimpleNamespace(
                id=f"task-{cid}",
                name=f"Task {cid}",
                description=f"Capability {cid}. " + "Detailed instructions. " * 60,
                expected_output=f"Full output for {cid}: " + "Output guidance. " * 80,
            )
            for cid in "abc"
        ]
    )
    return service


@pytest.mark.asyncio
async def test_selection_uses_one_compact_structured_request_after_interpreting_stages(
    requested_intent,
):
    service = generation_service()
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = plan("ab", []).model_dump_json()
        draft = await service.generate(
            FlowGenerationRequest(prompt="Two independent outcomes"),
            SimpleNamespace(group_ids=["team"]),
        )
    assert len(draft.nodes) == 2
    assert draft.edges == []
    complete.assert_awaited_once()
    kwargs = complete.await_args.kwargs
    payload = json.loads(kwargs["messages"][1]["content"])
    assert set(payload["catalog"]) == {"a", "b", "c"}
    assert "Full output" not in kwargs["messages"][1]["content"]
    assert len(kwargs["messages"][1]["content"]) < 1600
    # Reasoning models must retain their configured output allowance instead
    # of spending a hardcoded 6000-token limit before emitting any JSON.
    assert "max_tokens" not in kwargs
    assert kwargs["response_format"] is FlowPlanningStep


@pytest.mark.asyncio
async def test_details_are_loaded_only_for_selected_crews_when_requested():
    service = generation_service()
    detail_request = FlowPlanningStep(
        name="Check outputs",
        explanation="Need exact output fields",
        detail_crew_ids=["a"],
    )
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.side_effect = [
            detail_request.model_dump_json(),
            plan("ab", []).model_dump_json(),
        ]
        draft = await service.generate(
            FlowGenerationRequest(prompt="Check two crews"),
            SimpleNamespace(group_ids=["team"]),
        )
    assert len(draft.nodes) == 2
    assert complete.await_count == 2
    first, second = [
        json.loads(call.kwargs["messages"][1]["content"])
        for call in complete.await_args_list
    ]
    assert "crew_details" not in first
    assert list(second["crew_details"]) == ["a"]
    assert "Full output for a" in json.dumps(second)
    assert "Full output for b" not in json.dumps(second)
    assert "Full output for c" not in json.dumps(second)


@pytest.mark.asyncio
async def test_unknown_detail_id_is_repaired_without_exposing_other_crews(
    requested_intent,
):
    requested_intent.return_value = FlowIntent(stages=["Research"])
    service = generation_service()
    detail_request = FlowPlanningStep(
        name="Check", explanation="Check", detail_crew_ids=["other-team"]
    )
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.side_effect = [
            detail_request.model_dump_json(),
            plan("a", []).model_dump_json(),
        ]
        await service.generate(
            FlowGenerationRequest(prompt="Research"),
            SimpleNamespace(group_ids=["team"]),
        )
    payload = json.loads(complete.await_args.kwargs["messages"][1]["content"])
    assert "crew_details" not in payload
    assert "only for IDs" in payload["correction"]["error"]


@pytest.mark.asyncio
async def test_empty_answer_repair_keeps_compact_context_and_original_request():
    service = generation_service()
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.side_effect = ["", plan("ab", []).model_dump_json()]
        draft = await service.generate(
            FlowGenerationRequest(prompt="BM25 and Swiss news"),
            SimpleNamespace(group_ids=["team"]),
        )
    assert len(draft.nodes) == 2
    messages = complete.await_args.kwargs["messages"]
    assert len(messages) == 2
    payload = json.loads(messages[1]["content"])
    assert payload["prompt"] == "BM25 and Swiss news"
    assert "correction" in payload
    assert "Full output" not in messages[1]["content"]


@pytest.mark.asyncio
async def test_repeated_detail_requests_are_bounded():
    service = generation_service()
    detail_request = FlowPlanningStep(
        name="Check", explanation="Check", detail_crew_ids=["a"]
    )
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = detail_request.model_dump_json()
        with pytest.raises(ValueError, match="Could not generate a valid flow"):
            await service.generate(
                FlowGenerationRequest(prompt="Research"),
                SimpleNamespace(group_ids=["team"]),
            )
    assert complete.await_count == 3


@pytest.mark.asyncio
async def test_combined_crew_answer_is_repaired_before_it_reaches_the_canvas():
    service = generation_service()
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.side_effect = [
            plan("a", []).model_dump_json(),
            plan("ab", [{"source": "a", "target": "b"}]).model_dump_json(),
        ]
        draft = await service.generate(
            FlowGenerationRequest(
                prompt="Research news and make a presentation from it"
            ),
            SimpleNamespace(group_ids=["team"]),
        )
    assert len(draft.nodes) == 2
    assert len(draft.edges) == 1
    correction = json.loads(complete.await_args.kwargs["messages"][1]["content"])
    assert "Do not collapse stages" in correction["correction"]["error"]
    assert len(correction["requested_stages"]) == 2


def test_two_stage_assignments_cannot_point_to_the_same_combined_crew():
    proposal = plan("a", [])
    proposal.stage_assignments["stage_2"] = "a"
    with pytest.raises(ValueError, match="distinct crew"):
        validate_stage_assignments(
            proposal, FlowIntent(stages=["News", "Presentation"])
        )


@pytest.mark.asyncio
async def test_explicit_single_stage_request_can_still_use_a_combined_crew(
    requested_intent,
):
    requested_intent.return_value = FlowIntent(
        stages=["One crew to research news and make slides"]
    )
    service = generation_service()
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = plan("a", []).model_dump_json()
        draft = await service.generate(
            FlowGenerationRequest(prompt="Use one crew for news and slides"),
            SimpleNamespace(group_ids=["team"]),
        )
    assert len(draft.nodes) == 1
    assert draft.edges == []


@pytest.mark.asyncio
async def test_request_interpretation_happens_without_catalog_bias():
    with patch(
        "src.services.flow_builder.generation_intent.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = (
            '{"stages":["Research Swiss news","Create a presentation from the news"]}'
        )
        intent = await analyze_flow_intent(
            "research swiss news and make a presentation frmo it", "test-model"
        )
    assert len(intent.stages) == 2
    kwargs = complete.await_args.kwargs
    assert (
        kwargs["messages"][1]["content"]
        == "research swiss news and make a presentation frmo it"
    )
    assert kwargs["response_format"] is FlowIntent
    assert "catalog" not in kwargs["messages"][1]["content"]


@pytest.mark.asyncio
async def test_stage_interpretation_can_retain_the_existing_canvas_during_an_edit(
    requested_intent,
):
    service = generation_service()
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = plan(
            "ab", [{"source": "a", "target": "b"}]
        ).model_dump_json()
        await service.generate(
            FlowGenerationRequest(
                prompt="Add a presentation", current_crew_ids=["a", "other-team"]
            ),
            SimpleNamespace(group_ids=["team"]),
        )
    current = requested_intent.await_args.args[2]
    assert current == {"a": {"name": "Crew a", "tasks": ["Task a"]}}


@pytest.mark.asyncio
async def test_malformed_intent_is_retried_without_defaulting_to_one_crew():
    with patch(
        "src.services.flow_builder.generation_intent.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = '{"stages":[]}'
        with pytest.raises(ValueError, match="Could not identify"):
            await analyze_flow_intent("Research and create slides", "test-model")
    assert complete.await_count == 2


def news_contract():
    return {
        "crew_id": "a",
        "task_id": "task-a",
        "name": "News relevance",
        "schema_definition": {
            "type": "object",
            "required": ["articles"],
            "properties": {
                "articles": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["headline", "category", "importance"],
                        "properties": {
                            "headline": {"type": "string"},
                            "category": {
                                "type": "string",
                                "enum": ["policy", "sport"],
                                "description": "Classify the article topic from its evidence",
                            },
                            "importance": {
                                "type": "integer",
                                "minimum": 0,
                                "maximum": 10,
                                "description": "Importance to the team on a zero to ten scale",
                            },
                        },
                    },
                }
            },
        },
    }


def routed_news_plan():
    return FlowPlanningStep(
        name="Relevant news",
        explanation="Send important policy news to the team; otherwise stop.",
        crew_ids=["a", "b"],
        output_contracts=[news_contract()],
        stage_assignments={"stage_1": "a", "stage_2": "b"},
        links=[
            {
                "source": "a",
                "target": "b",
                "condition_groups": [
                    {
                        "subject": "articles",
                        "terms": [
                            {"field": "category", "operator": "==", "value": "policy"},
                            {"field": "importance", "operator": ">=", "value": 7},
                        ],
                    }
                ],
            }
        ],
    )


def test_generated_schema_routes_same_article_and_survives_serialization():
    from src.schemas.flow_generation import FlowGenerationResponse
    from src.services.flow_builder.modules.flow_conditions import (
        ConditionState,
        make_where,
    )
    from src.utils.safe_eval import safe_eval

    original = catalog()
    snapshot = json.dumps(original)
    response = build_flow(routed_news_plan(), original)
    restored = FlowGenerationResponse.model_validate_json(response.model_dump_json())
    assert restored.nodes[0].data.model_dump()["outputContract"] == news_contract()
    assert json.dumps(original) == snapshot  # No catalog mutation.
    condition = restored.edges[0].data["routerCondition"]

    def matches(articles):
        state = ConditionState({"articles": articles})
        return bool(
            safe_eval(
                condition,
                {"state": state, "where": make_where(state)},
                allowed_call_names=frozenset({"where"}),
            )
        )

    assert matches([{"category": "policy", "importance": 8}])
    assert not matches(
        [
            {"category": "policy", "importance": 2},
            {"category": "sport", "importance": 9},
        ]
    )
    assert not matches([])
    assert len(restored.edges) == 1  # No invented fallback crew.


@pytest.mark.parametrize(
    "change", ["crew", "task", "field", "value", "reference", "duplicate"]
)
def test_bad_output_contracts_and_routes_are_rejected(change):
    data = routed_news_plan().model_dump()
    if change == "crew":
        data["output_contracts"][0]["crew_id"] = "other-team"
    elif change == "task":
        data["output_contracts"][0]["task_id"] = "task-b"
    elif change == "field":
        data["links"][0]["condition_groups"][0]["terms"][0]["field"] = "invented"
    elif change == "value":
        data["links"][0]["condition_groups"][0]["terms"][1]["value"] = "urgent"
    elif change == "reference":
        data["output_contracts"][0]["schema_definition"][
            "$ref"
        ] = "https://example.com/schema"
    else:
        data["output_contracts"].append(news_contract())
    with pytest.raises(ValueError):
        build_flow(FlowPlanningStep.model_validate(data), catalog())


@pytest.mark.asyncio
@pytest.mark.parametrize("harness_name", ["kasal", "crewai"])
async def test_flow_contract_reaches_real_task_schema_on_both_harnesses(harness_name):
    from pydantic import ValidationError

    from src.services.execution.harnesses import active_harness
    from src.services.execution.harnesses.selection import bind
    from src.services.execution.kernel.task_builder import build_task_args
    from src.services.flow_builder.modules.task_adapter import TaskConfig
    from src.services.flow_builder.output_contracts import apply_flow_output_contract

    flow = json.loads(build_flow(routed_news_plan(), catalog()).model_dump_json())
    task_data = SimpleNamespace(
        id="task-a",
        name="News",
        description="Research news",
        expected_output="A complete news report",
        config={},
    )
    spec = apply_flow_output_contract(
        TaskConfig._task_data_to_spec(task_data), task_data.id, flow
    )
    assert task_data.config == {}
    assert "complete news report" in spec["expected_output"]
    with bind(harness_name):
        args = await build_task_args(spec, None, [])
        task = active_harness().build_task(**args)
        assert task.output_json is not None
        task.output_json.model_validate(
            {"articles": [{"headline": "News", "category": "policy", "importance": 8}]}
        )
        with pytest.raises(ValidationError):
            task.output_json.model_validate(
                {
                    "articles": [
                        {"headline": "News", "category": "policy", "importance": 99}
                    ]
                }
            )
        with pytest.raises(ValidationError):
            task.output_json.model_validate({"articles": [{"headline": "News"}]})
    untouched = apply_flow_output_contract(
        {"expected_output": "Original"}, "task-b", flow
    )
    assert untouched == {"expected_output": "Original"}


@pytest.mark.asyncio
async def test_generation_creates_contract_after_bounded_source_detail_request():
    service = generation_service()
    detail = FlowPlanningStep(
        name="Check source", explanation="Inspect source output", detail_crew_ids=["a"]
    )
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.side_effect = [
            detail.model_dump_json(),
            routed_news_plan().model_dump_json(),
        ]
        response = await service.generate(
            FlowGenerationRequest(prompt="Send important policy news to our team"),
            SimpleNamespace(group_ids=["team"]),
        )
    assert len(response.nodes) == 2
    assert response.nodes[0].data.model_dump()["outputContract"]["task_id"] == "task-a"
    assert complete.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("task_id", [1, "1", "", None, "task-b"])
async def test_generation_binds_final_task_instead_of_retrying_model_task_ids(task_id):
    service = generation_service()
    data = routed_news_plan().model_dump()
    data["output_contracts"][0]["task_id"] = task_id
    with patch(
        "src.services.flow_builder.generation.LLMManager.completion",
        new_callable=AsyncMock,
    ) as complete:
        complete.return_value = json.dumps(data)
        result = await service.generate(
            FlowGenerationRequest(prompt="Route important news"),
            SimpleNamespace(group_ids=["team"]),
        )
    assert result.nodes[0].data.model_dump()["outputContract"]["task_id"] == "task-a"
    assert len(result.nodes) == 2
    complete.assert_awaited_once()


def test_generation_binding_does_not_authorize_an_unselected_or_unknown_crew():
    data = routed_news_plan().model_dump()
    data["output_contracts"][0]["crew_id"] = "other-team"
    with pytest.raises(ValueError, match="selected crew"):
        draft = FlowPlanningStep.model_validate(data, context={"catalog": catalog()})
        build_flow(draft, catalog())


def test_generation_binding_handles_missing_task_id_without_mutating_answer():
    data = routed_news_plan().model_dump()
    del data["output_contracts"][0]["task_id"]
    draft = FlowPlanningStep.model_validate(data, context={"catalog": catalog()})
    assert draft.output_contracts[0].task_id == "task-a"
    assert "task_id" not in data["output_contracts"][0]


@pytest.mark.asyncio
async def test_selected_capabilities_are_assigned_per_flow_task_without_editing_saved_crews():
    service = generation_service()
    with (
        patch(
            "src.services.flow_builder.generation.describe_selected_mcps",
            new_callable=AsyncMock,
            return_value=[{"name": "postgres"}, {"name": "studio"}],
        ),
        patch(
            "src.services.flow_builder.generation.describe_selected_tools",
            new_callable=AsyncMock,
            return_value=[{"name": "tool:4", "tool_id": "4"}],
        ),
        patch(
            "src.services.flow_builder.generation.assign_mcps_to_tasks",
            new_callable=AsyncMock,
            return_value={"a/task-a": ["postgres", "tool:4"], "b/task-b": ["studio"]},
        ) as assign,
        patch(
            "src.services.flow_builder.generation.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=plan("ab", [{"source": "a", "target": "b"}]).model_dump_json(),
        ),
    ):
        draft = await service.generate(
            FlowGenerationRequest(
                prompt="Research and create slides",
                mcp_servers=["postgres", "studio"],
                tools=["4"],
            ),
            SimpleNamespace(group_ids=["group"]),
        )
    assert draft.nodes[0].data.mcpAssignments == {"task-a": ["postgres"]}
    assert draft.nodes[1].data.mcpAssignments == {"task-b": ["studio"]}
    assert draft.nodes[0].data.toolAssignments == {"task-a": ["4"]}
    assert draft.nodes[1].data.toolAssignments == {"task-b": []}
    assert "mcpAssignments" in draft.model_dump()["nodes"][0]["data"]
    assert set(assign.call_args.args[0]) == {"a/task-a", "b/task-b"}
    assert not hasattr(service.tasks.find_by_group_ids.return_value[0], "tool_configs")
