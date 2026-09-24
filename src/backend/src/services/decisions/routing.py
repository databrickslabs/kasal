"""Jev chooses among published capabilities; the LLM still extracts arguments."""

from src.services.decisions.policies import select


async def routing_candidates(message, capabilities, turns, group_id=None):
    choice = await select(
        "workflow_dispatch",
        {
            "message": message,
            "conversation": [{"role": t.role, "content": t.preview} for t in turns],
        },
        [
            {"name": c.name, "description": c.description, "inputs": c.input_schema}
            for c in capabilities
        ],
        group_id=group_id,
    )
    if choice is None:
        return capabilities
    return [] if choice == -1 else [capabilities[choice]]


async def follow_up_target(message, turns, group_id=None):
    answers = [t for t in turns if t.role == "assistant"]
    choice = await select(
        "follow_up_target",
        {
            "message": message,
            "task": "Which earlier answer does the user explicitly ask to reuse or transform? Choose no candidate for a new independent request.",
        },
        [t.preview for t in answers],
        group_id=group_id,
    )
    if choice is None:
        return None
    return [] if choice == -1 else [answers[choice].index]
