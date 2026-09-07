"""The post-run renderer must use the same ownership rules as Chat."""

from unittest.mock import AsyncMock

import pytest

from src.services.a2ui import runner


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prompt", ["Create a presentation", "Draw an architecture diagram"]
)
async def test_html_deliverable_does_not_invoke_composer(monkeypatch, prompt):
    compose = AsyncMock()
    monkeypatch.setattr(runner, "compose_surface", compose)
    result = {"output": '<section class="slide">Title</section>'}
    assert (
        await runner.wrap_result_with_surface(
            result, config={"tasks": [{"description": prompt}]}
        )
        is result
    )
    compose.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["quiz", "mindmap", "flashcards"])
@pytest.mark.parametrize("flow", [False, True])
async def test_crew_and_flow_pass_app_deliverables_to_shared_composer(
    monkeypatch, kind, flow
):
    task = {"description": f"Create a {kind} about LLMs"}
    config = {"nodes": [{"data": {"allTasks": [task]}}]} if flow else {"tasks": [task]}
    surface = {"surfaceKind": kind, "components": []}
    compose = AsyncMock(return_value=surface)
    monkeypatch.setattr(runner, "compose_surface", compose)
    result = "```html\n<div>Questions and answers</div>\n```"
    output = await runner.wrap_result_with_surface(
        result, config=config, group_id="team-a"
    )
    assert output["a2ui"] == surface
    assert kind in compose.call_args.kwargs["query"]
    assert compose.call_args.kwargs["group_id"] == "team-a"
