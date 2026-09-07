"""Crew and flow task output ownership must match Chat, under either harness."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.a2ui.output_directive import apply_diagram_directive
from src.services.execution.kernel.task_builder import build_task_args


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prompt,kind",
    [
        ("Create a quiz about LLMs", "quiz"),
        ("Create flashcards about biology", "flashcards"),
        ("Create a mindmap of machine learning", "mindmap"),
        ("Show cities on a map", "map"),
        ("Build a dashboard of sales", "dashboard"),
    ],
)
async def test_tasks_get_chat_structured_contract_without_mutating_catalog(
    prompt, kind
):
    spec = {
        "name": prompt,
        "description": prompt,
        "expected_output": "The requested deliverable",
    }
    saved = deepcopy(spec)
    agent = MagicMock()
    args = await build_task_args(spec, agent, [])
    chat = apply_diagram_directive({}, prompt)["backstory"]
    assert chat in args["description"]
    assert chat in args["expected_output"]
    assert f"The app builds the {kind}" in args["description"]
    assert "%md-sandbox diagram specialist" not in args["description"]
    assert spec == saved
    # Subsequent research by the SAME agent isn't turned into a quiz/deck.
    other = await build_task_args(
        {"description": "Gather facts", "expected_output": "Findings"}, agent, []
    )
    assert other["description"] == "Gather facts"


@pytest.mark.asyncio
async def test_diagram_contract_matches_chat():
    prompt = "Draw an architecture diagram"
    args = await build_task_args({"description": prompt}, MagicMock(), [])
    assert apply_diagram_directive({}, prompt)["backstory"] in args["description"]
    assert "SLIDE DECK DESIGN SYSTEM" not in args["description"]


@pytest.mark.asyncio
async def test_deck_uses_same_teamspace_palette_as_chat():
    palette = {"themes": {"presentation": {"accent": "#AB47BC"}}}
    # UIConfig stores style_json, which resolve_themes reads in both paths.
    import json

    config = SimpleNamespace(enabled=True, style_json=json.dumps(palette))
    with (
        patch("src.services.settings.ui.UIConfigService") as service,
        patch("src.db.session.routed_scoped_session") as session,
    ):
        session.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        session.return_value.__aexit__ = AsyncMock(return_value=False)
        service.return_value.get_config = AsyncMock(return_value=config)
        prompt = "Create a presentation about LLMs"
        args = await build_task_args(
            {"description": prompt}, MagicMock(), [], config={"group_id": "team-a"}
        )
        assert service.call_args.kwargs["group_id"] == "team-a"
        assert (
            apply_diagram_directive({}, prompt, themes=palette["themes"])["backstory"]
            in args["description"]
        )
        assert "#AB47BC" in args["description"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "contract", ["output_schema", "output_json", "output_pydantic"]
)
async def test_presentation_does_not_override_machine_contracts(contract):
    from src.services.execution.kernel.task_presentation import apply_task_presentation

    args = {"description": "Create a quiz", "expected_output": "JSON matching schema"}
    before = dict(args)
    await apply_task_presentation(args, {**args, contract: {"type": "object"}})
    assert args == before
