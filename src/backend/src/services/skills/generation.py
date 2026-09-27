"""Draft an Agent Skill from a request or a conversation — a generation call.

Creating a skill is a product action, not agent knowledge: the same shape as
crew generation (a DB-backed prompt template, one focused LLM call, JSON out,
validated before anyone sees it) rather than a meta-skill the agent has to
decide to load mid-turn and then obey. The model only PROPOSES; the draft goes
back to the chat as a card whose Save button is the human commit gate.

Two modes, one call:
- **capture** — the request arrives with the conversation transcript; the
  corrections the user made ARE the skill, so the template mines those first.
- **blank page** — the request alone; thin requests still get a best draft with
  an "Open questions" section rather than an interview (the user refines by
  asking again).
"""

import json
import logging
from contextlib import nullcontext
from typing import Any, Dict, List, Optional, Tuple

from src.core.llm.robust_json import robust_json_parser
from src.services.catalog.templates import TemplateService
from src.services.llm.manager import LLMManager
from src.services.otel_tracing.generation_scope import (
    generation_step,
    generation_trace,
)
from src.services.skills import draft_run, parser
from src.utils.telemetry import KasalProduct, get_user_agent_header
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

TEMPLATE_NAME = "generate_skill"
#: Transcript turns kept for capture mode (the tail of the conversation).
MAX_TRANSCRIPT_TURNS = 30
MAX_TURN_CHARS = 4000
#: How the draft reads in its trace: the lane (agent) and its step (task).
TRACE_LABEL = "Skills"
TRACE_STEP = "Draft the skill"
RETRY_STEP = "Fix validation errors"


class SkillGenerationService:
    """One focused LLM call → a validated ``{name, description, body}`` draft."""

    @staticmethod
    async def draft(
        request: str,
        group_context: Optional[GroupContext],
        *,
        transcript: Optional[List[Dict[str, str]]] = None,
        model: Optional[str] = None,
        session: Any = None,
        job_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """The validated draft, plus how it was made: the served ``model``,
        the ``attempts`` it took and the ``job_id`` of the run whose trace
        records its LLM calls (see :mod:`draft_run`).

        Pass ``job_id`` when the run is already open (:mod:`draft_job` opens it
        first so the trace is readable while the model works); otherwise one is
        opened with ``session`` — None when there is no session or the record
        could not be written, and the draft goes on untraced."""
        system = await _system_prompt(group_context)
        user = _user_message(request, transcript)
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        if job_id is None:
            job_id = await draft_run.open_run(
                session,
                request=request,
                transcript_turns=len(transcript or []),
                model=model,
                group_context=group_context,
            )
        try:
            # The calls go through the event bus like every other LLM call; the
            # scoped OTel bridge turns them into this run's trace rows (request,
            # response, model, tokens, timing) — no hand-written rows.
            async with _traced(job_id, group_context):
                draft, served = await _ask(messages, model)
                verdict = _validate(draft)
                attempts = 1
                if not verdict["valid"]:
                    # One retry with the validator's own words: the errors are
                    # exact (name shape, missing description) and the model
                    # fixes them reliably when told; a second failure is
                    # returned as-is so the card can show the same messages.
                    logger.info(
                        "[skills] draft failed validation, retrying once: %s",
                        verdict["errors"],
                    )
                    messages.append({"role": "assistant", "content": json.dumps(draft)})
                    messages.append(
                        {
                            "role": "user",
                            "content": "That draft failed validation:\n- "
                            + "\n- ".join(verdict["errors"])
                            + "\nReturn the corrected JSON object only.",
                        }
                    )
                    with generation_step(RETRY_STEP):
                        draft, served = await _ask(messages, model)
                    verdict = _validate(draft)
                    attempts = 2
        except Exception as exc:
            await draft_run.close_run(job_id, error=str(exc) or exc.__class__.__name__)
            raise
        result = {
            **draft,
            **verdict,
            "model": served,
            "attempts": attempts,
            "job_id": job_id,
        }
        await draft_run.close_run(job_id, result=result)
        return result


def _traced(job_id: Optional[str], group_context: Optional[GroupContext]) -> Any:
    """The run's trace scope, or nothing when there is no run to file under."""
    if not job_id:
        return nullcontext()
    return generation_trace(job_id, group_context, TRACE_LABEL, TRACE_STEP)


async def _system_prompt(group_context: Optional[GroupContext]) -> str:
    """The DB-backed template (group/user overrides apply), else the seed."""
    try:
        if group_context is not None:
            content = await TemplateService.get_effective_template_content(
                TEMPLATE_NAME, group_context
            )
            if content and content.strip():
                return content
    except Exception as exc:  # noqa: BLE001 — a template read must not block a draft
        logger.warning("[skills] template %r unavailable: %s", TEMPLATE_NAME, exc)
    from src.seeds.prompt_templates import GENERATE_SKILL_TEMPLATE

    return GENERATE_SKILL_TEMPLATE


def _user_message(request: str, transcript: Optional[List[Dict[str, str]]]) -> str:
    turns = [
        t
        for t in (transcript or [])
        if isinstance(t, dict)
        and t.get("role") in ("user", "assistant")
        and t.get("content")
    ][-MAX_TRANSCRIPT_TURNS:]
    if not turns:
        return f"MODE: blank page\n\nREQUEST:\n{request.strip()}"
    lines = [
        f"{t['role'].upper()}: {str(t['content'])[:MAX_TURN_CHARS]}" for t in turns
    ]
    return (
        "MODE: capture — mine this conversation. What the user corrected, "
        "rejected or asked for twice becomes the skill's first rules; what they "
        "accepted without comment is the output shape.\n\n"
        f"REQUEST:\n{request.strip() or 'Save what we learned in this conversation as a skill.'}\n\n"
        "CONVERSATION:\n" + "\n\n".join(lines)
    )


async def _ask(
    messages: List[Dict[str, str]], model: Optional[str]
) -> Tuple[Dict[str, Any], Optional[str]]:
    """One call: the parsed ``{name, description, body}`` and the model that
    served it (the resolved one — a picker key may be substituted)."""
    content, served = await LLMManager.completion(
        messages=messages,
        model=model,
        temperature=0.4,
        max_tokens=4000,
        extra_headers=get_user_agent_header(KasalProduct.SKILL),
        with_served_model=True,
    )
    try:
        data = robust_json_parser(content or "")
    except Exception:  # noqa: BLE001 — unparseable reply -> empty, invalid draft
        data = {}
    if not isinstance(data, dict):
        data = {}
    return (
        {
            "name": str(data.get("name") or "").strip(),
            "description": str(data.get("description") or "").strip(),
            "body": str(data.get("body") or "").strip(),
        },
        _served_name(served, model),
    )


def _served_name(served: Any, requested: Optional[str]) -> Optional[str]:
    """The served model as a plain name. ``completion`` reports a substitution
    as ``"<served> (for '<requested>')"``; with no requested model that
    reads ``(for 'None')``, which is noise, so only the served half stays."""
    if not isinstance(served, str) or not served.strip():
        return requested or None
    if requested:
        return served
    return served.split(" (for ", 1)[0]


def _validate(draft: Dict[str, Any]) -> Dict[str, Any]:
    """The reference validator's verdict, in the shape the card understands."""
    try:
        parsed = parser.validate_row(draft["name"], draft["description"], draft["body"])
        return {
            "valid": True,
            "errors": [],
            "warnings": list(getattr(parsed, "warnings", None) or []),
        }
    except Exception as exc:  # noqa: BLE001 — SkillValidationError or a bad field
        errors = getattr(exc, "errors", None)
        return {
            "valid": False,
            "errors": [str(e) for e in errors] if errors else [str(exc)],
            "warnings": [],
        }
