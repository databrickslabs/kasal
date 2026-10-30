from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.decisions import policies
from src.services.decisions.contracts import Choice


def choice(value):
    return Choice(value, 0.99, {value: 0.99})


@pytest.mark.asyncio
async def test_task_assignment_uses_only_selected_names_and_keeps_empty_assignments():
    with (
        patch.object(
            policies,
            "decide",
            new=AsyncMock(
                return_value={
                    "0_0": choice("yes"),
                    "0_1": choice("no"),
                    "1_0": choice("no"),
                    "1_1": choice("no"),
                }
            ),
        ),
        patch("src.services.generation.mcp_assignment.LLMManager.completion") as old,
    ):
        from src.services.generation.mcp_assignment import assign_mcps_to_tasks

        assert await assign_mcps_to_tasks(
            {"read": {}, "write": {}},
            [{"name": "database"}, {"name": "browser"}],
            "model",
        ) == {"read": ["database"], "write": []}
        old.assert_not_called()


@pytest.mark.asyncio
async def test_ranking_preserves_objects_metadata_and_stable_order():
    records = [{"content": "a", "private": "owner"}, {"content": "b"}, {"content": "c"}]
    with patch.object(
        policies,
        "decide",
        new=AsyncMock(
            return_value={
                "0": choice("no"),
                "1": choice("yes"),
                "2": choice("yes"),
            }
        ),
    ):
        result = await policies.rank(
            "knowledge", "request", records, ["a", "b", "c"], group_id="one"
        )
    assert result == [records[1], records[2], records[0]]
    assert result[2] is records[0]


@pytest.mark.asyncio
async def test_disabled_ranking_returns_original_list():
    records = ["a", "b"]
    with patch.object(policies, "decide", new=AsyncMock(return_value=None)):
        assert await policies.rank("knowledge", "request", records, records) is records


@pytest.mark.asyncio
async def test_route_selection_restricts_to_authorized_candidate_and_no_match():
    from src.services.decisions.routing import routing_candidates

    caps = [
        SimpleNamespace(name="a", description="A", input_schema={}),
        SimpleNamespace(name="b", description="B", input_schema={}),
    ]
    with patch.object(
        policies, "decide", new=AsyncMock(return_value={"selection": choice("1")})
    ):
        assert await routing_candidates("request", caps, [], "one") == [caps[1]]
    with patch.object(
        policies, "decide", new=AsyncMock(return_value={"selection": choice("none")})
    ):
        assert await routing_candidates("request", caps, [], "one") == []


@pytest.mark.asyncio
async def test_follow_up_only_references_assistant_turns():
    from src.services.decisions.routing import follow_up_target

    turns = [
        SimpleNamespace(role="user", index=1, preview="hi"),
        SimpleNamespace(role="assistant", index=2, preview="answer"),
    ]
    with patch.object(
        policies, "decide", new=AsyncMock(return_value={"selection": choice("0")})
    ) as decide:
        assert await follow_up_target("edit it", turns, "one") == [2]
        assert decide.call_args.args[1]["candidates"] == ["answer"]


@pytest.mark.asyncio
async def test_output_format_does_not_override_html_or_deck():
    from src.services.decisions.output import surface_kind

    with patch(
        "src.services.decisions.output.decide", new_callable=AsyncMock
    ) as decide:
        assert await surface_kind("make a presentation", "", "answer") is None
        assert (
            await surface_kind("answer", "", "```html\n<section>hi</section>\n```")
            is None
        )
        decide.assert_not_awaited()


def test_memory_adapter_changes_only_classification():
    from src.services.decisions.memory import MemoryDecisions
    from src.services.memory.engine.analyze import MemoryAnalysis

    analysis = MemoryAnalysis(categories=["work"], importance=0.7)
    with patch("src.services.decisions.memory.classify_sync", return_value="semantic"):
        result = MemoryDecisions("one").label("fact", analysis)
    assert result.kind == "semantic"
    assert result.categories == ["work"] and result.importance == 0.7
    assert analysis.kind == "episodic"


def test_memory_supersession_never_targets_a_newer_record():
    from src.services.decisions.memory import MemoryDecisions

    records = [SimpleNamespace(content="new"), SimpleNamespace(content="old")]
    with patch(
        "src.services.decisions.memory.decide_sync", return_value={"1": choice("0")}
    ) as decide:
        assert MemoryDecisions("one").supersession(records) == [
            {"current": 0, "outdated": [1]}
        ]
        assert set(decide.call_args.args[2]["1"]["criteria"]) == {"0", "none"}


@pytest.mark.asyncio
async def test_evidence_review_adds_warning_without_inventing_sources():
    from src.services.decisions.output import evidence_guidance

    with patch(
        "src.services.decisions.output.decide",
        new=AsyncMock(return_value={"support": choice("unsupported")}),
    ):
        assert "unsupported" in await evidence_guidance("request", "draft", ["source"])


def test_task_goal_context_is_restored_after_errors():
    from src.services.decisions.context import request_goal, task_goal

    with pytest.raises(RuntimeError):
        with task_goal("goal"):
            assert request_goal.get() == "goal"
            raise RuntimeError()
    assert request_goal.get() == ""
