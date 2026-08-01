"""Answering a turn from work already done, without running anything.

Some turns ask ABOUT what an earlier turn produced — "which frameworks did you
find?", "what did the second one say?". The material is already in state, put
there by crews that have already run and been paid for. Running a crew to retell
it spends minutes reproducing something sitting in memory, and the answer is
worse: a fresh run gathers again and may not even find the same things.

So a turn can end without executing any part of the graph. The flow returns a
written answer and the run finishes.

Why the answer is written here rather than by chat's light agent
===============================================================

The light agent is the better answerer — it owns the prompt, the budget, the
guardrails and the streaming — and the honest design hands this back to it. That
would mean a new result shape travelling the whole pipeline (subprocess →
execution status → SSE → chat) and the chat deciding, before the run starts,
whether the flow's state can answer a question it cannot see.

This is the smaller correct step: ONE short call, over material the flow already
has, with no tools and no crew. It is deliberately not a second agent — no
memory, no tool loop, no retries — because the moment it grows those it has
become a duplicate of the light agent and should be replaced by it.
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: Channels that are bookkeeping rather than material a question is about.
_NOT_MATERIAL = {
    "id",
    "messages",
    "last_user_message",
    "last_intent",
    "last_outcome",
    "session_ready",
    "summary",
    "previous_output",
    # Named explicitly, not caught by the leading-underscore rule below: the
    # channel cannot start with one and still survive serialization (see
    # reuse.IDENTITY_CHANNEL). Without this line the hashes would be handed to
    # the model as if they were material a question could be about.
    "kasal_crew_identities",
}

#: Per-channel cap. A crew output can be a whole report; the answer needs enough
#: to be accurate, not the entire corpus in one prompt.
MATERIAL_CHAR_CAP = 4000

_INSTRUCTIONS = (
    "You answer a question using ONLY the material below, which a workflow has "
    "already produced. Do not invent anything that is not there. If the "
    "material does not contain the answer, say so plainly and name what IS "
    "there — that is a useful answer, and guessing is not. Be direct and keep "
    "the shape the question asks for."
)


def material_from_state(state: Any) -> Dict[str, str]:
    """What the flow has produced so far, by the name that produced it.

    Bookkeeping channels are excluded: a question is about the work, and putting
    the conversation and the identity hashes in front of the model wastes the
    budget that should go to the material itself.
    """
    try:
        dump = state.model_dump() if hasattr(state, "model_dump") else dict(state)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[flow-retrieval] could not read state: {exc}")
        return {}

    material: Dict[str, str] = {}
    for key, value in (dump or {}).items():
        if key in _NOT_MATERIAL or str(key).startswith("_"):
            continue
        if value in (None, "", [], {}):
            continue
        text = value if isinstance(value, str) else json.dumps(value, default=str)
        material[str(key)] = text[:MATERIAL_CHAR_CAP]
    return material


def render_material(material: Dict[str, str]) -> str:
    return "\n\n".join(f"### {name}\n{text}" for name, text in sorted(material.items()))


def build_messages(
    question: str, material: Dict[str, str], recent: str = ""
) -> List[Dict[str, str]]:
    """The one call: instructions, the material, the turn."""
    body = f"Material already produced:\n{render_material(material)}\n\n"
    if recent:
        body += f"The conversation so far:\n{recent}\n\n"
    return [
        {"role": "system", "content": _INSTRUCTIONS},
        {"role": "user", "content": body + f"The question:\n{question}"},
    ]


async def answer_from_state(
    question: str, state: Any, model: Optional[str] = None
) -> Optional[str]:
    """An answer written from what the flow already holds, or None.

    None means "this cannot be answered without running", and the caller runs
    the flow as it otherwise would. Every failure returns None: no material, no
    model, an empty reply. A turn that silently produced nothing would be worse
    than a slow one.
    """
    material = material_from_state(state)
    if not question or not material:
        return None

    try:
        from src.services.flow_builder.conversation.outcomes import render_recent
        from src.services.llm.manager import LLMManager

        response = await LLMManager.completion(
            messages=build_messages(
                question, material, render_recent(getattr(state, "messages", None))
            ),
            model=model,
        )
        content = (
            response["choices"][0]["message"]["content"]
            if isinstance(response, dict)
            else str(response)
        )
    except Exception as exc:  # noqa: BLE001 — fall back to running the flow
        logger.warning(f"[flow-retrieval] could not answer from state: {exc}")
        return None

    answer = (content or "").strip()
    if not answer:
        return None
    logger.info(
        "[flow-retrieval] answered from %d stored output(s) without running anything",
        len(material),
    )
    return answer
