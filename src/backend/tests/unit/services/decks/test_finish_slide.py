from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.services.decks.finish_slide import capture_evidence, finish_slide

SLIDE = '<section class="slide">Revised</section>'


@pytest.mark.asyncio
async def test_finalizes_using_actual_evidence_without_repeating_tools():
    tools = [object()]
    agent = SimpleNamespace(tools=tools, max_retry_limit=2)
    config = object()

    async def finalization(received_agent, messages, received_config, *args):
        assert received_agent is agent
        assert agent.tools == []
        assert agent.max_retry_limit == 0
        assert received_config is config
        text = str(messages)
        assert "Original slide" in text
        assert "https://example.org/source" in text
        assert "Now I'll create the improved slide" in text
        assert "not a promise or plan" in text
        return SimpleNamespace(raw=SLIDE)

    service = SimpleNamespace(
        _kickoff_with_mlflow_trace=AsyncMock(side_effect=finalization)
    )
    result = await finish_slide(
        service,
        agent,
        SimpleNamespace(raw="Now I'll create the improved slide"),
        "Original slide",
        ["Browser: https://example.org/source — verified text"],
        config,
        "run",
        "trace",
        None,
        "group",
        Mock(),
    )
    assert result.raw == SLIDE
    service._kickoff_with_mlflow_trace.assert_awaited_once()
    assert agent.tools is tools
    assert agent.max_retry_limit == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kicked",
    [SimpleNamespace(raw=SLIDE), SimpleNamespace(raw="partial", budget_exhausted=True)],
)
async def test_valid_or_exhausted_run_is_never_retried(kicked):
    service = SimpleNamespace(_kickoff_with_mlflow_trace=AsyncMock())
    assert (
        await finish_slide(
            service, None, kicked, "", [], None, "", "", None, "", Mock()
        )
        is kicked
    )
    service._kickoff_with_mlflow_trace.assert_not_awaited()


@pytest.mark.asyncio
async def test_restores_tools_when_finalization_fails():
    tools = [object()]
    agent = SimpleNamespace(tools=tools, max_retry_limit=2)
    service = SimpleNamespace(
        _kickoff_with_mlflow_trace=AsyncMock(side_effect=RuntimeError("failed"))
    )
    with pytest.raises(RuntimeError, match="failed"):
        await finish_slide(
            service,
            agent,
            SimpleNamespace(raw="promise"),
            "",
            [],
            None,
            "",
            "",
            None,
            "",
            Mock(),
        )
    assert agent.tools is tools
    assert agent.max_retry_limit == 2
    service._kickoff_with_mlflow_trace.assert_awaited_once()


def test_evidence_is_bounded_and_marks_missing_content():
    evidence = []
    for i in range(20):
        capture_evidence(
            evidence, "browser", f"url-{i}", "START" + "x" * 100000 + "END"
        )
    assert sum(map(len, evidence)) <= 100000
    assert "url-19" in evidence[-1]
    assert "START" in evidence[-1] and "END" in evidence[-1]
    assert "excerpt omitted" in evidence[-1]


@pytest.mark.asyncio
async def test_finalization_keeps_original_deadline_and_honors_exhaustion():
    from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
    from src.services.chat.turn_kickoff import kickoff_chat_turn

    agent = SimpleNamespace(tools=[object()], max_retry_limit=2)
    deadlines = []

    async def kickoff(*args):
        deadlines.append(agent.run_deadline)
        if len(deadlines) == 1:
            return SimpleNamespace(raw="Now I'll create the slide")
        raise ExecutionBudgetExceededError("deadline reached", partial="Partial")

    service = SimpleNamespace(_kickoff_with_mlflow_trace=AsyncMock(side_effect=kickoff))
    result = await kickoff_chat_turn(
        service,
        agent,
        SimpleNamespace(
            inputs={"execution_effort": {"tier": "low"}}, output_contract="slide"
        ),
        "run",
        "trace",
        None,
        "group",
        "Revise",
        {},
        "",
        None,
        Mock(),
        output_evidence=["source evidence"],
    )
    assert len(deadlines) == 2
    assert deadlines[0] == deadlines[1]
    assert result.budget_exhausted is True
    assert len(agent.tools) == 1
